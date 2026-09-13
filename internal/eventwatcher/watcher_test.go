package eventwatcher

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"

	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// setupStores connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema, and
// returns an invoice.Store and webhook.Store both backed by the same real Postgres
// pool — matching Phase 1a's DI precedent of only faking the pieces that talk to an
// external system (here: the wallet event stream and the HTTP sender), not the
// database layer. Skips the calling test if no live Postgres DSN is configured.
func setupStores(t *testing.T) (*invoice.Store, *webhook.Store) {
	t.Helper()
	dsn := os.Getenv("TARIPAY_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SKIP: TARIPAY_TEST_POSTGRES_DSN not set, no live Postgres to test against")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	database := &db.DB{Pool: pool}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations, webhook_deliveries, invoices CASCADE`); err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}
	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	invoiceStore := invoice.NewStore(pool, func(paymentID string) (string, error) {
		return "fake-address-for-" + paymentID, nil
	})
	return invoiceStore, webhook.NewStore(pool)
}

// spySender records every Send call (without performing real HTTP requests) and
// always reports success — sufficient for this package's tests, which only assert
// HOW MANY TIMES a webhook was fired and for which invoices, not delivery-outcome
// bookkeeping (that's webhook package's own TestAttempt_* tests' job).
type spySender struct {
	mu    sync.Mutex
	calls int
}

func (s *spySender) Send(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return 200, nil
}

func (s *spySender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// fakeEventSource returns an EventSourceFunc that yields exactly the given events (in
// order) on a buffered channel, then closes both channels cleanly with no error —
// simulating a normal end-of-stream. This exercises the same channel-closing sequence
// go-tari-lib's real StreamTransactionEvents uses (errCh closes, then eventsCh
// closes), per that function's own doc comment.
func fakeEventSource(evs []*tari_generated.TransactionEventResponse) EventSourceFunc {
	return func(ctx context.Context) (<-chan *tari_generated.TransactionEventResponse, <-chan error) {
		events := make(chan *tari_generated.TransactionEventResponse, len(evs))
		errs := make(chan error)
		for _, ev := range evs {
			events <- ev
		}
		close(errs)
		close(events)
		return events, errs
	}
}

// inboundEvent/outboundEvent's Direction fixtures use "Inbound"/"Outbound" — the
// SHORT form tari_generated.TransactionEvent.Direction actually uses on the wire
// (confirmed live against a real minotari_console_wallet on 2026-09-13; see
// transactionEventDirectionInbound's doc comment in watcher.go). Earlier revisions
// of these fixtures used the long enum-name form
// ("TRANSACTION_DIRECTION_INBOUND"/"_OUTBOUND"), which is what
// tari_generated.TransactionInfo.Direction (a different struct, from
// GetCompletedTransactions) uses — that mismatch let watcher.go's Direction check
// ship broken against real traffic while these mock-fixture-based tests kept passing
// against the same wrong assumption. Don't revert this back to the long form.
func inboundEvent(paymentID, status, txID string) *tari_generated.TransactionEventResponse {
	return &tari_generated.TransactionEventResponse{
		Transaction: &tari_generated.TransactionEvent{
			Event:         "Received",
			TxId:          txID,
			Status:        status,
			Direction:     "Inbound",
			Amount:        1000,
			UserPaymentId: []byte(paymentID),
		},
	}
}

func outboundEvent(paymentID, status, txID string) *tari_generated.TransactionEventResponse {
	return &tari_generated.TransactionEventResponse{
		Transaction: &tari_generated.TransactionEvent{
			Event:         "Sent",
			TxId:          txID,
			Status:        status,
			Direction:     "Outbound",
			Amount:        1000,
			UserPaymentId: []byte(paymentID),
		},
	}
}

// TestRun_FullSequence exercises the brief's required scenario: an event for an
// unrelated/unknown payment_id, a first-time "seen" transition, a first-time
// "confirmed" transition, a repeat "confirmed" event that must NOT refire a webhook,
// and a "rejected" transition on a separate invoice.
func TestRun_FullSequence(t *testing.T) {
	invoiceStore, webhookStore := setupStores(t)
	ctx := context.Background()

	invB, err := invoiceStore.Create(ctx, "order-b", 5000, time.Hour)
	if err != nil {
		t.Fatalf("create invB: %v", err)
	}
	invC, err := invoiceStore.Create(ctx, "order-c", 7000, time.Hour)
	if err != nil {
		t.Fatalf("create invC: %v", err)
	}
	invA, err := invoiceStore.Create(ctx, "order-a", 100, time.Hour)
	if err != nil {
		t.Fatalf("create invA (untouched control): %v", err)
	}

	// Status fixtures use "Broadcast"/"Mined Confirmed"/"Rejected" — the short,
	// space-separated form tari_generated.TransactionEvent.Status actually uses on
	// the wire (confirmed live on 2026-09-13; see mapStatus's doc comment in
	// watcher.go). The long TRANSACTION_STATUS_* enum-name form is what
	// tari_generated.TransactionInfo.Status (from GetCompletedTransactions) uses,
	// not this field.
	events := []*tari_generated.TransactionEventResponse{
		inboundEvent("unrelated-payment-id-does-not-exist", "Broadcast", "tx-unrelated"),
		outboundEvent(invB.PaymentID, "Mined Confirmed", "tx-outbound-ignored"),
		inboundEvent(invB.PaymentID, "Broadcast", "tx-b-1"),              // first-time seen
		inboundEvent(invB.PaymentID, "Mined Confirmed", "tx-b-2"),        // first-time confirmed
		inboundEvent(invB.PaymentID, "Mined Confirmed", "tx-b-2-repeat"), // repeat confirmed - must NOT refire
		inboundEvent(invC.PaymentID, "Rejected", "tx-c-1"),               // rejected
	}

	spy := &spySender{}
	w := NewWatcher(invoiceStore, webhookStore, spy, "https://merchant.example/webhook", fakeEventSource(events))

	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil (clean end of stream)", err)
	}

	gotB, err := invoiceStore.GetByID(ctx, invB.ID)
	if err != nil {
		t.Fatalf("GetByID(invB) error = %v", err)
	}
	if gotB.Status != invoice.StatusConfirmed {
		t.Errorf("invB.Status = %q, want %q", gotB.Status, invoice.StatusConfirmed)
	}

	gotC, err := invoiceStore.GetByID(ctx, invC.ID)
	if err != nil {
		t.Fatalf("GetByID(invC) error = %v", err)
	}
	if gotC.Status != invoice.StatusRejected {
		t.Errorf("invC.Status = %q, want %q", gotC.Status, invoice.StatusRejected)
	}

	gotA, err := invoiceStore.GetByID(ctx, invA.ID)
	if err != nil {
		t.Fatalf("GetByID(invA) error = %v", err)
	}
	if gotA.Status != invoice.StatusPending {
		t.Errorf("invA.Status = %q, want %q (untouched by unrelated event)", gotA.Status, invoice.StatusPending)
	}

	// Exactly 3 webhooks fired: invB seen, invB confirmed, invC rejected. The
	// repeat confirmed event for invB must NOT have fired a 4th.
	if got := spy.callCount(); got != 3 {
		t.Errorf("spy.callCount() = %d, want 3 (seen + confirmed + rejected, no refire on repeat confirmed)", got)
	}

	deliveries, err := webhookStore.ListRecent(ctx, 10)
	if err != nil {
		t.Fatalf("ListRecent() error = %v", err)
	}
	if len(deliveries) != 3 {
		t.Fatalf("len(deliveries) = %d, want 3", len(deliveries))
	}
	for _, d := range deliveries {
		if d.Status != webhook.StatusDelivered {
			t.Errorf("delivery %s status = %q, want %q", d.ID, d.Status, webhook.StatusDelivered)
		}
	}
}

// TestRun_NoCallbackURLSkipsWebhookButStillUpdatesStatus verifies invoice status
// updates happen even when webhook delivery is unconfigured (empty callbackURL) —
// per NewWatcher's doc comment, this must not be fatal.
func TestRun_NoCallbackURLSkipsWebhookButStillUpdatesStatus(t *testing.T) {
	invoiceStore, webhookStore := setupStores(t)
	ctx := context.Background()

	inv, err := invoiceStore.Create(ctx, "order-nocallback", 100, time.Hour)
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}

	events := []*tari_generated.TransactionEventResponse{
		inboundEvent(inv.PaymentID, "Mined Confirmed", "tx-1"),
	}
	spy := &spySender{}
	w := NewWatcher(invoiceStore, webhookStore, spy, "", fakeEventSource(events))

	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	got, err := invoiceStore.GetByID(ctx, inv.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != invoice.StatusConfirmed {
		t.Errorf("Status = %q, want %q", got.Status, invoice.StatusConfirmed)
	}
	if spy.callCount() != 0 {
		t.Errorf("spy.callCount() = %d, want 0 (no callback URL configured)", spy.callCount())
	}

	deliveries, err := webhookStore.ListRecent(ctx, 10)
	if err != nil {
		t.Fatalf("ListRecent() error = %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("len(deliveries) = %d, want 0", len(deliveries))
	}
}

// TestRun_StreamErrorIsReturned verifies a non-nil error from the error channel is
// logged and returned as-is (no retry logic inside Run itself).
func TestRun_StreamErrorIsReturned(t *testing.T) {
	invoiceStore, webhookStore := setupStores(t)

	wantErr := errors.New("stream: connection lost")
	events := func(ctx context.Context) (<-chan *tari_generated.TransactionEventResponse, <-chan error) {
		eventsCh := make(chan *tari_generated.TransactionEventResponse)
		errCh := make(chan error, 1)
		errCh <- wantErr
		close(errCh)
		close(eventsCh)
		return eventsCh, errCh
	}

	spy := &spySender{}
	w := NewWatcher(invoiceStore, webhookStore, spy, "https://merchant.example/webhook", events)

	err := w.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Errorf("Run() error = %v, want %v", err, wantErr)
	}
}

// TestRun_ContextCancellationReturnsContextErr verifies Run returns promptly with
// ctx.Err() when ctx is cancelled and the stream never produces anything.
func TestRun_ContextCancellationReturnsContextErr(t *testing.T) {
	invoiceStore, webhookStore := setupStores(t)

	events := func(ctx context.Context) (<-chan *tari_generated.TransactionEventResponse, <-chan error) {
		// Channels that never yield anything and never close - only
		// ctx.Done() can end Run() in this scenario.
		return make(chan *tari_generated.TransactionEventResponse), make(chan error)
	}

	spy := &spySender{}
	w := NewWatcher(invoiceStore, webhookStore, spy, "https://merchant.example/webhook", events)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
}
