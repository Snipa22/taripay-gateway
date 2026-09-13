package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
)

// setupStore connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema, creates
// one invoice row (webhook_deliveries.invoice_id has a NOT NULL FK to invoices), and
// returns a webhook Store plus that invoice's ID for tests to reference. Skips the
// calling test if no live Postgres DSN is configured — same convention as
// internal/invoice/invoice_test.go's setupStore.
func setupStore(t *testing.T) (*Store, uuid.UUID) {
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
	inv, err := invoiceStore.Create(ctx, "order-webhook-test", 1000, time.Hour)
	if err != nil {
		t.Fatalf("create fixture invoice: %v", err)
	}

	return NewStore(pool), inv.ID
}

func TestStore_CreateAndGetByID(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	payload := []byte(`{"event":"payment.seen"}`)
	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", payload)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if d.Status != StatusPending {
		t.Errorf("Status = %q, want %q", d.Status, StatusPending)
	}
	if d.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", d.Attempts)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.InvoiceID != invoiceID {
		t.Errorf("InvoiceID = %v, want %v", got.InvoiceID, invoiceID)
	}
	if got.CallbackURL != "https://merchant.example/webhook" {
		t.Errorf("CallbackURL = %q, want %q", got.CallbackURL, "https://merchant.example/webhook")
	}
	// Postgres's jsonb column normalizes whitespace on round-trip (e.g. adds a
	// space after ":"), so compare semantically rather than as exact bytes.
	var gotParsed, wantParsed any
	if err := json.Unmarshal(got.Payload, &gotParsed); err != nil {
		t.Fatalf("unmarshal got.Payload: %v", err)
	}
	if err := json.Unmarshal(payload, &wantParsed); err != nil {
		t.Fatalf("unmarshal want payload: %v", err)
	}
	if !reflect.DeepEqual(gotParsed, wantParsed) {
		t.Errorf("Payload = %s, want (semantically) %s", got.Payload, payload)
	}
}

func TestStore_GetByID_NotFound(t *testing.T) {
	s, _ := setupStore(t)
	ctx := context.Background()

	_, err := s.GetByID(ctx, uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetByID() error = %v, want ErrNotFound", err)
	}
}

func TestStore_ListRecent(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	var ids []uuid.UUID
	for i := 0; i < 3; i++ {
		d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		ids = append(ids, d.ID)
		time.Sleep(time.Millisecond) // ensure distinct created_at ordering
	}

	got, err := s.ListRecent(ctx, 2)
	if err != nil {
		t.Fatalf("ListRecent() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListRecent() len = %d, want 2", len(got))
	}
	// Newest first.
	if got[0].ID != ids[2] {
		t.Errorf("ListRecent()[0].ID = %v, want %v (newest)", got[0].ID, ids[2])
	}
	if got[1].ID != ids[1] {
		t.Errorf("ListRecent()[1].ID = %v, want %v", got[1].ID, ids[1])
	}
}

func TestStore_MarkDelivered(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := s.MarkDelivered(ctx, d.ID, 200); err != nil {
		t.Fatalf("MarkDelivered() error = %v", err)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusDelivered {
		t.Errorf("Status = %q, want %q", got.Status, StatusDelivered)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 200 {
		t.Errorf("ResponseCode = %v, want 200", got.ResponseCode)
	}
	if got.DeliveredAt == nil {
		t.Error("DeliveredAt is nil, want a timestamp")
	}

	if err := s.MarkDelivered(ctx, uuid.New(), 200); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkDelivered(unknown id) error = %v, want ErrNotFound", err)
	}
}

func TestStore_MarkFailed(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	code := 500
	if err := s.MarkFailed(ctx, d.ID, &code, "server error"); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, StatusFailed)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 500 {
		t.Errorf("ResponseCode = %v, want 500", got.ResponseCode)
	}
	if got.LastError == nil || *got.LastError != "server error" {
		t.Errorf("LastError = %v, want %q", got.LastError, "server error")
	}
	if got.DeliveredAt != nil {
		t.Errorf("DeliveredAt = %v, want nil (never delivered)", got.DeliveredAt)
	}

	// A transport-level failure (no status code at all).
	d2, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := s.MarkFailed(ctx, d2.ID, nil, "connection refused"); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	got2, err := s.GetByID(ctx, d2.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got2.ResponseCode != nil {
		t.Errorf("ResponseCode = %v, want nil", got2.ResponseCode)
	}
}

func TestStore_IncrementAttempts(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := s.IncrementAttempts(ctx, d.ID); err != nil {
			t.Fatalf("IncrementAttempts() error = %v", err)
		}
		got, err := s.GetByID(ctx, d.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if got.Attempts != i {
			t.Errorf("Attempts = %d, want %d", got.Attempts, i)
		}
	}

	if err := s.IncrementAttempts(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("IncrementAttempts(unknown id) error = %v, want ErrNotFound", err)
	}
}

// TestSender_Send_SignsPayloadCorrectly verifies Sender.Send computes the
// X-TariPay-Signature header correctly, using an httptest.NewServer to capture the
// actual outgoing request rather than mocking the HTTP client.
func TestSender_Send_SignsPayloadCorrectly(t *testing.T) {
	const secret = "test-hmac-secret"
	payload := []byte(`{"event":"payment.confirmed","invoice_id":"abc-123"}`)

	var gotSignature, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSignature = r.Header.Get("X-TariPay-Signature")
		gotContentType = r.Header.Get("Content-Type")
		buf := make([]byte, len(payload)+16)
		n, _ := r.Body.Read(buf)
		gotBody = buf[:n]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sender := NewSender(secret)
	statusCode, err := sender.Send(context.Background(), srv.URL, payload)
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("statusCode = %d, want %d", statusCode, http.StatusOK)
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if gotSignature != wantSignature {
		t.Errorf("X-TariPay-Signature = %q, want %q", gotSignature, wantSignature)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", gotContentType, "application/json")
	}
	if string(gotBody) != string(payload) {
		t.Errorf("request body = %s, want %s", gotBody, payload)
	}
}

// TestSender_Send_NonTransportErrorReturnsStatusCode verifies a non-2xx response is
// surfaced as a status code, not an error — deciding what to do with it is the
// caller's job (see Attempt).
func TestSender_Send_NonTransportErrorReturnsStatusCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sender := NewSender("secret")
	statusCode, err := sender.Send(context.Background(), srv.URL, []byte(`{}`))
	if err != nil {
		t.Fatalf("Send() error = %v, want nil (a 500 response is not a transport error)", err)
	}
	if statusCode != http.StatusInternalServerError {
		t.Errorf("statusCode = %d, want %d", statusCode, http.StatusInternalServerError)
	}
}

// TestSender_Send_TransportFailureReturnsError verifies an unreachable callback URL
// (nothing listening) surfaces as a non-nil error with a zero status code.
func TestSender_Send_TransportFailureReturnsError(t *testing.T) {
	sender := NewSender("secret")
	_, err := sender.Send(context.Background(), "http://127.0.0.1:1", []byte(`{}`))
	if err == nil {
		t.Fatal("Send() error = nil, want a transport-level error for an unreachable URL")
	}
}

// spySender is a fake SenderInterface implementation used by TestAttempt_* below to
// verify Attempt's increment/send/mark sequence without any real HTTP calls.
type spySender struct {
	statusCode int
	err        error
	calls      int
}

func (s *spySender) Send(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	s.calls++
	return s.statusCode, s.err
}

func TestAttempt_SuccessMarksDelivered(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	spy := &spySender{statusCode: 200}
	if err := Attempt(ctx, s, spy, d); err != nil {
		t.Fatalf("Attempt() error = %v", err)
	}
	if spy.calls != 1 {
		t.Errorf("spy.calls = %d, want 1", spy.calls)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusDelivered {
		t.Errorf("Status = %q, want %q", got.Status, StatusDelivered)
	}
	if got.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", got.Attempts)
	}
}

func TestAttempt_TransportFailureMarksFailed(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	spy := &spySender{err: errors.New("connection refused")}
	if err := Attempt(ctx, s, spy, d); err != nil {
		t.Fatalf("Attempt() error = %v, want nil (a failed delivery is a handled outcome)", err)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, StatusFailed)
	}
	if got.LastError == nil || *got.LastError != "connection refused" {
		t.Errorf("LastError = %v, want %q", got.LastError, "connection refused")
	}
	if got.ResponseCode != nil {
		t.Errorf("ResponseCode = %v, want nil", got.ResponseCode)
	}
}

func TestAttempt_NonSuccessStatusMarksFailed(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	d, err := s.Create(ctx, invoiceID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	spy := &spySender{statusCode: 503}
	if err := Attempt(ctx, s, spy, d); err != nil {
		t.Fatalf("Attempt() error = %v", err)
	}

	got, err := s.GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, StatusFailed)
	}
	if got.ResponseCode == nil || *got.ResponseCode != 503 {
		t.Errorf("ResponseCode = %v, want 503", got.ResponseCode)
	}
}

// seedDelivery inserts a webhook_deliveries row with a fully-controlled
// status/attempts/created_at, bypassing Store.Create (which always sets attempts=0,
// created_at=now()) — needed for TestRetryFailedDeliveries below to construct rows
// that are/aren't eligible for retry along every one of ListRetryable's three axes
// (status, age, attempts count).
func seedDelivery(t *testing.T, s *Store, invoiceID uuid.UUID, status string, attempts int, createdAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := s.db.Exec(context.Background(), `
		INSERT INTO webhook_deliveries (id, invoice_id, callback_url, payload, status, attempts, created_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
	`, id, invoiceID, "https://merchant.example/webhook", `{}`, status, attempts, createdAt)
	if err != nil {
		t.Fatalf("seedDelivery: %v", err)
	}
	return id
}

// TestRetryFailedDeliveries seeds a mix of pending/failed/delivered rows — some
// within maxAge, some older, some at (or past) the attempts cap — and confirms
// RetryFailedDeliveries only calls sender.Send for the eligible ones, returns a count
// matching exactly that set, and leaves every ineligible row completely untouched
// (status/attempts unchanged, no Send call for it).
func TestRetryFailedDeliveries(t *testing.T) {
	s, invoiceID := setupStore(t)
	ctx := context.Background()

	const maxAge = 24 * time.Hour
	now := time.Now().UTC()

	// Eligible: failed, recent, under the attempts cap.
	eligibleFailed := seedDelivery(t, s, invoiceID, StatusFailed, 1, now.Add(-time.Hour))
	// Eligible: pending (never even attempted), recent, under the attempts cap.
	eligiblePending := seedDelivery(t, s, invoiceID, StatusPending, 0, now.Add(-time.Minute))
	// Ineligible: failed, but older than maxAge.
	tooOld := seedDelivery(t, s, invoiceID, StatusFailed, 1, now.Add(-48*time.Hour))
	// Ineligible: failed, recent, but already at the attempts cap.
	atCap := seedDelivery(t, s, invoiceID, StatusFailed, DefaultRetryMaxAttempts, now.Add(-time.Hour))
	// Ineligible: already delivered.
	delivered := seedDelivery(t, s, invoiceID, StatusDelivered, 1, now.Add(-time.Hour))

	spy := &spySender{statusCode: 200}
	count, err := RetryFailedDeliveries(ctx, s, spy, maxAge)
	if err != nil {
		t.Fatalf("RetryFailedDeliveries() error = %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2 (only eligibleFailed + eligiblePending)", count)
	}
	if spy.calls != 2 {
		t.Errorf("spy.calls = %d, want 2", spy.calls)
	}

	for _, id := range []uuid.UUID{eligibleFailed, eligiblePending} {
		got, err := s.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("GetByID(%s) error = %v", id, err)
		}
		if got.Status != StatusDelivered {
			t.Errorf("delivery %s Status = %q, want %q (must have been retried and succeeded)", id, got.Status, StatusDelivered)
		}
	}

	for _, tc := range []struct {
		name         string
		id           uuid.UUID
		wantStatus   string
		wantAttempts int
	}{
		{"tooOld", tooOld, StatusFailed, 1},
		{"atCap", atCap, StatusFailed, DefaultRetryMaxAttempts},
		{"delivered", delivered, StatusDelivered, 1},
	} {
		got, err := s.GetByID(ctx, tc.id)
		if err != nil {
			t.Fatalf("GetByID(%s) error = %v", tc.name, err)
		}
		if got.Status != tc.wantStatus {
			t.Errorf("%s: Status = %q, want %q (must be untouched)", tc.name, got.Status, tc.wantStatus)
		}
		if got.Attempts != tc.wantAttempts {
			t.Errorf("%s: Attempts = %d, want %d (must be untouched)", tc.name, got.Attempts, tc.wantAttempts)
		}
	}
}
