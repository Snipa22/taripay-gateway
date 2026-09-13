// Package webhook implements Phase 1b's webhook delivery log (Store, backed by the
// webhook_deliveries table — see internal/db/migrations/0002_webhook_deliveries.up.sql)
// and outbound delivery (Sender, an HMAC-signed HTTP POST to a merchant-configured
// callback URL).
//
// Store and Sender are deliberately separate: Store is pure persistence (no network
// calls), Sender is pure network (no persistence) — internal/eventwatcher and
// cmd/gateway's admin retry route both compose the two via the Attempt helper below
// rather than either type reaching into the other.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned by GetByID when no matching delivery row exists.
var ErrNotFound = errors.New("webhook: not found")

// Status values for the webhook_deliveries.status column.
const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
)

// Delivery mirrors the `webhook_deliveries` table (see
// internal/db/migrations/0002_webhook_deliveries.up.sql).
type Delivery struct {
	ID           uuid.UUID
	InvoiceID    uuid.UUID
	CallbackURL  string
	Payload      []byte
	Status       string
	Attempts     int
	LastError    *string
	ResponseCode *int
	CreatedAt    time.Time
	DeliveredAt  *time.Time
}

// Payload is the JSON body shape POSTed to a merchant's webhook callback URL, per the
// task brief's "Webhook payload JSON shape" spec. Event distinguishes what triggered
// this particular delivery (e.g. "payment.seen", "payment.confirmed",
// "payment.rejected", "payment.underpaid") since a merchant may receive multiple
// webhooks for one invoice as it transitions between statuses.
type Payload struct {
	InvoiceID   string `json:"invoice_id"`
	PaymentID   string `json:"payment_id"`
	OrderRef    string `json:"order_ref"`
	Status      string `json:"status"`
	AmountUTari uint64 `json:"amount_utari"`
	TxID        string `json:"tx_id"`
	ConfirmedAt string `json:"confirmed_at,omitempty"`
	Event       string `json:"event"`

	// AmountReceivedUTari is a C2-fix addition (task brief "fix C2 and add
	// auth", part 1): the cumulative amount actually received so far for this
	// invoice (invoice.Invoice.AmountReceivedUTari at the time this webhook
	// fired), alongside AmountUTari above which remains the *invoiced* amount.
	// Only meaningfully populated for "payment.underpaid" (and any subsequent
	// "payment.confirmed" that followed an underpayment) — it lets the
	// merchant's plugin show both numbers (invoiced vs. received) rather than
	// just "something went wrong".
	AmountReceivedUTari uint64 `json:"amount_received_utari,omitempty"`
}

// Store wraps a *pgxpool.Pool with this package's webhook-delivery persistence
// operations.
type Store struct {
	db *pgxpool.Pool
}

// NewStore builds a Store backed by db.
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Create inserts a new pending delivery row for invoiceID and returns the resulting
// Delivery (id/status/attempts/created_at all populated as inserted).
func (s *Store) Create(ctx context.Context, invoiceID uuid.UUID, callbackURL string, payload []byte) (*Delivery, error) {
	return createDelivery(ctx, s.db, invoiceID, callbackURL, payload)
}

// CreateTx is Create run inside a caller-managed transaction tx instead of against
// this Store's own pool directly. Added for the webhook-delivery-reliability fix
// (task brief part 2, item 1): internal/eventwatcher's updateStatusAndCreateDelivery
// composes this with invoice.Store.UpdateStatusTx inside one pgx.Tx so the invoice
// status transition and its corresponding webhook delivery row commit together or
// not at all — it must be impossible for the invoice to transition without a
// corresponding delivery row existing (even a never-attempted one, for
// RetryFailedDeliveries to eventually find). tx must have been started against the
// same underlying database this Store's pool points at (true for every call site in
// this repo; see invoice.Store.Pool's doc comment).
func (s *Store) CreateTx(ctx context.Context, tx pgx.Tx, invoiceID uuid.UUID, callbackURL string, payload []byte) (*Delivery, error) {
	return createDelivery(ctx, tx, invoiceID, callbackURL, payload)
}

// execer is the subset of *pgxpool.Pool's and pgx.Tx's identical Exec signature this
// package needs — satisfied by both, so createDelivery below can run either as a
// standalone statement against the pool (Create) or as one statement inside a
// caller-managed transaction (CreateTx), sharing the exact same SQL/error handling
// either way.
type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func createDelivery(ctx context.Context, db execer, invoiceID uuid.UUID, callbackURL string, payload []byte) (*Delivery, error) {
	d := &Delivery{
		ID:          uuid.New(),
		InvoiceID:   invoiceID,
		CallbackURL: callbackURL,
		Payload:     payload,
		Status:      StatusPending,
		Attempts:    0,
		CreatedAt:   time.Now().UTC(),
	}

	// payload is inserted via an explicit ::jsonb cast on the placeholder rather
	// than relying on pgx's driver-level type inference for a []byte parameter
	// against a jsonb column (which otherwise risks being sent as bytea and
	// rejected by Postgres) — string(payload) is safe here since payload is
	// always valid UTF-8 JSON produced by json.Marshal in this package's callers.
	_, err := db.Exec(ctx, `
		INSERT INTO webhook_deliveries (id, invoice_id, callback_url, payload, status, attempts, created_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
	`, d.ID, d.InvoiceID, d.CallbackURL, string(d.Payload), d.Status, d.Attempts, d.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("webhook: insert: %w", err)
	}
	return d, nil
}

// GetByID looks up a delivery by its primary key. Returns ErrNotFound if no such row
// exists.
func (s *Store) GetByID(ctx context.Context, id uuid.UUID) (*Delivery, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, invoice_id, callback_url, payload, status, attempts, last_error, response_code, created_at, delivered_at
		FROM webhook_deliveries WHERE id = $1
	`, id)
	return scanDelivery(row)
}

// ListRecent returns the most recently created deliveries, newest first, capped at
// limit rows.
func (s *Store) ListRecent(ctx context.Context, limit int) ([]*Delivery, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, invoice_id, callback_url, payload, status, attempts, last_error, response_code, created_at, delivered_at
		FROM webhook_deliveries
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("webhook: list recent: %w", err)
	}
	defer rows.Close()

	var out []*Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webhook: list recent: rows: %w", err)
	}
	return out, nil
}

// MarkDelivered records a successful delivery: status -> delivered, response_code set,
// delivered_at set to now(). Returns ErrNotFound if no such row exists.
func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, responseCode int) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = $2, response_code = $3, delivered_at = now()
		WHERE id = $1
	`, id, StatusDelivered, responseCode)
	if err != nil {
		return fmt.Errorf("webhook: mark delivered: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkFailed records a failed delivery attempt: status -> failed, last_error set, and
// response_code set if statusCode is non-nil (nil for a transport-level failure that
// never got a response at all, e.g. connection refused). delivered_at is left NULL.
// Returns ErrNotFound if no such row exists.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, statusCode *int, errMsg string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = $2, response_code = $3, last_error = $4
		WHERE id = $1
	`, id, StatusFailed, statusCode, errMsg)
	if err != nil {
		return fmt.Errorf("webhook: mark failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// IncrementAttempts bumps a delivery's attempts counter by one. Called once per send
// attempt (initial delivery and every manual retry), independent of the outcome.
// Returns ErrNotFound if no such row exists.
func (s *Store) IncrementAttempts(ctx context.Context, id uuid.UUID) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE webhook_deliveries SET attempts = attempts + 1 WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("webhook: increment attempts: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListRetryable returns every delivery eligible for an automatic background retry
// (task brief part 2, item 2): status pending or failed, created within the last
// maxAge, and with fewer than maxAttempts attempts so far. Oldest-first, so a long
// backlog is worked through in the order it accumulated rather than newest-first.
func (s *Store) ListRetryable(ctx context.Context, maxAge time.Duration, maxAttempts int) ([]*Delivery, error) {
	cutoff := time.Now().UTC().Add(-maxAge)
	rows, err := s.db.Query(ctx, `
		SELECT id, invoice_id, callback_url, payload, status, attempts, last_error, response_code, created_at, delivered_at
		FROM webhook_deliveries
		WHERE status IN ($1, $2) AND created_at > $3 AND attempts < $4
		ORDER BY created_at ASC
	`, StatusFailed, StatusPending, cutoff, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("webhook: list retryable: %w", err)
	}
	defer rows.Close()

	var out []*Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webhook: list retryable: rows: %w", err)
	}
	return out, nil
}

// rowScanner is the subset of pgx.Row's/pgx.Rows' interface scanDelivery needs —
// satisfied by both QueryRow's return value and a *pgx.Rows during Next() iteration,
// mirroring internal/invoice's own rowScanner precedent.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelivery(row rowScanner) (*Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.InvoiceID, &d.CallbackURL, &d.Payload, &d.Status, &d.Attempts, &d.LastError, &d.ResponseCode, &d.CreatedAt, &d.DeliveredAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("webhook: scan: %w", err)
	}
	return &d, nil
}

// SenderInterface is the subset of *Sender's behavior internal/eventwatcher and
// cmd/gateway's admin retry route need. Defined here (rather than those packages
// depending on the concrete *Sender directly) so tests can inject a spy that records
// calls without performing real HTTP requests — *Sender satisfies this trivially,
// since its Send method already matches this exact signature.
type SenderInterface interface {
	Send(ctx context.Context, callbackURL string, payload []byte) (statusCode int, err error)
}

// Sender POSTs webhook payloads to merchant callback URLs, HMAC-signing each one.
type Sender struct {
	httpClient *http.Client
	hmacSecret string
}

// NewSender builds a Sender that signs outgoing payloads with hmacSecret and uses a
// 10s request timeout — long enough for a merchant's plugin to do its own signature
// verification and a small amount of work, short enough that one slow/unreachable
// merchant endpoint can't stall the event-watcher loop indefinitely.
func NewSender(hmacSecret string) *Sender {
	return &Sender{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		hmacSecret: hmacSecret,
	}
}

// Send POSTs payload to callbackURL with an X-TariPay-Signature header
// ("sha256=<hex HMAC-SHA256 of payload using hmacSecret>") and
// Content-Type: application/json. Returns the response status code on any completed
// HTTP round trip (2xx or not — deciding what to do with a non-2xx is the caller's
// job, see the Attempt helper below) and a non-nil error only on a transport-level
// failure (DNS, connection refused, timeout) where no status code was ever obtained.
//
// Constant-time comparison of the signature is the RECEIVING side's job (the WooCommerce
// plugin, built separately) — this method only needs to compute the HMAC correctly on
// the sending side, which crypto/hmac + crypto/sha256 already does safely.
func (s *Sender) Send(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	mac := hmac.New(sha256.New, []byte(s.hmacSecret))
	mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("webhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TariPay-Signature", "sha256="+signature)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("webhook: send: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on an already-completed response; failure here is inconsequential.
	// Drain and discard the response body so the connection can be reused by the
	// underlying transport; we don't care about the merchant endpoint's response
	// content, only its status code.
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

// Attempt performs one delivery attempt for delivery: increments its attempt counter,
// sends it via sender, and records the outcome (MarkDelivered on any 2xx response,
// MarkFailed otherwise — including on a transport-level Send error, which is recorded
// in the store but NOT returned by Attempt, since a failed delivery is an expected,
// fully-handled outcome, not a caller bug). Attempt's own (non-nil) return value is
// reserved for infra-level failures — the Store writes themselves failing — which
// callers (internal/eventwatcher, cmd/gateway's admin retry route) should log and treat
// as this attempt's bookkeeping being unreliable, not just "the webhook failed".
//
// This is shared by both the event-watcher's initial delivery attempt and the admin
// UI's manual retry route so the increment/send/mark sequence lives in exactly one
// place.
func Attempt(ctx context.Context, store *Store, sender SenderInterface, delivery *Delivery) error {
	if err := store.IncrementAttempts(ctx, delivery.ID); err != nil {
		return fmt.Errorf("webhook: attempt: increment attempts: %w", err)
	}

	statusCode, err := sender.Send(ctx, delivery.CallbackURL, delivery.Payload)
	if err != nil {
		if markErr := store.MarkFailed(ctx, delivery.ID, nil, err.Error()); markErr != nil {
			return fmt.Errorf("webhook: attempt: mark failed after send error (%v): %w", err, markErr)
		}
		return nil
	}

	if statusCode >= 200 && statusCode < 300 {
		if markErr := store.MarkDelivered(ctx, delivery.ID, statusCode); markErr != nil {
			return fmt.Errorf("webhook: attempt: mark delivered: %w", markErr)
		}
		return nil
	}

	errMsg := fmt.Sprintf("non-2xx response: %d", statusCode)
	if markErr := store.MarkFailed(ctx, delivery.ID, &statusCode, errMsg); markErr != nil {
		return fmt.Errorf("webhook: attempt: mark failed after non-2xx response: %w", markErr)
	}
	return nil
}

// DefaultRetryMaxAttempts caps how many total attempts (the initial delivery attempt
// plus every retry, automatic or manual, combined) a delivery gets before
// RetryFailedDeliveries stops picking it up automatically. An operator can still
// force one more attempt past this cap via the admin UI's manual retry button (which
// calls Attempt directly and does not consult this cap at all) — this only bounds the
// *automatic* background retry loop, so a permanently-broken merchant callback URL
// doesn't get hammered forever unattended.
//
// PLACEHOLDER, UNCONFIRMED default (same "reasonable-sounding guess, not
// project-owner-approved" caveat as this repo's other placeholder defaults —
// config.DefaultConfirmationDepth/DefaultInvoiceTTLMinutes,
// cmd/gateway's eventWatcherMaxBackoff): 10 attempts, chosen so a genuinely transient
// outage (a merchant's plugin/server restarting, a brief network blip) gets several
// tries across the default 60s ticker interval (cmd/gateway's retryTickerInterval)
// before this gateway gives up on it unattended, without retrying an obviously-dead
// endpoint indefinitely.
const DefaultRetryMaxAttempts = 10

// DefaultRetryMaxAge bounds how far back RetryFailedDeliveries looks for eligible
// pending/failed deliveries — rows older than this are treated as abandoned rather
// than retried forever. A merchant callback URL that has been broken for longer than
// this is a configuration problem worth a human operator's attention (via the admin
// UI's webhook-deliveries log and its manual retry button), not something this
// gateway should keep silently retrying in the background indefinitely.
//
// PLACEHOLDER, UNCONFIRMED default, same caveat as DefaultRetryMaxAttempts above: 24h
// is a reasonable-sounding guess for "long enough to ride out a weekend outage,
// short enough not to retry ancient rows forever" — not a value the project owner
// (Alex) has explicitly signed off on.
const DefaultRetryMaxAge = 24 * time.Hour

// RetryFailedDeliveries is the automatic background retry loop (task brief part 2,
// item 2): it queries store for every delivery currently eligible for retry (status
// pending or failed, created within the last maxAge, attempts below
// DefaultRetryMaxAttempts — see Store.ListRetryable) and calls Attempt for each one in
// turn, via the exact same increment/send/mark sequence used by both the
// event-watcher's initial delivery attempt and the admin UI's manual retry button —
// this function does not duplicate that logic, only decides which rows to run it
// against and when.
//
// Returns the count of deliveries actually processed (i.e. Attempt was called for
// them) — regardless of whether that attempt itself then succeeded or failed;
// "processed" means "given a retry try", not "delivered". A non-nil error is only
// returned for an infra-level failure listing eligible rows in the first place (a
// Store.ListRetryable failure); a per-delivery Attempt bookkeeping failure is logged
// and that delivery is skipped (not counted, and not fatal to the rest of the batch)
// rather than aborting the whole retry pass over one bad row — same
// "log and keep going" precedent as internal/eventwatcher's handleEvent.
//
// This does NOT replace the admin UI's manual retry button (internal/admin's
// handleWebhookRetry) — that remains available for an operator who wants to force an
// immediate retry outside this function's periodic cycle (see
// cmd/gateway/main.go's retry-ticker goroutine, which calls this on a fixed
// interval). Both paths share the same underlying Attempt call, per the task brief's
// explicit "don't duplicate the increment/send/mark logic" instruction.
func RetryFailedDeliveries(ctx context.Context, store *Store, sender SenderInterface, maxAge time.Duration) (int, error) {
	deliveries, err := store.ListRetryable(ctx, maxAge, DefaultRetryMaxAttempts)
	if err != nil {
		return 0, fmt.Errorf("webhook: retry failed deliveries: %w", err)
	}

	processed := 0
	for _, d := range deliveries {
		if err := Attempt(ctx, store, sender, d); err != nil {
			log.Printf("webhook: retry failed deliveries: attempt bookkeeping for delivery %s: %v (skipping)", d.ID, err)
			continue
		}
		processed++
	}
	return processed, nil
}
