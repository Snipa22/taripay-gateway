package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"

	"github.com/Snipa22/taripay-gateway/internal/admin"
	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// setupTestServer connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema, and
// returns an httptest.Server wired to a fake wallet-address resolver (per resolveErr:
// nil for success, non-nil to exercise the "wallet address resolution failed" error
// path). Skips the calling test if no live Postgres DSN is configured.
func setupTestServer(t *testing.T, resolveErr error) *httptest.Server {
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

	resolveAddress := func(paymentID string) (string, error) {
		if resolveErr != nil {
			return "", resolveErr
		}
		return "fake-address-for-" + paymentID, nil
	}
	store := invoice.NewStore(pool, resolveAddress)
	handler := newHandler(store, 30*time.Minute)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func TestPostInvoice_Success(t *testing.T) {
	srv := setupTestServer(t, nil)

	body, _ := json.Marshal(createInvoiceRequest{OrderRef: "order-abc", AmountUTari: 1000})
	resp, err := http.Post(srv.URL+"/invoice", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /invoice: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	var got invoiceResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.OrderRef != "order-abc" {
		t.Errorf("OrderRef = %q, want %q", got.OrderRef, "order-abc")
	}
	if got.AmountUTari != 1000 {
		t.Errorf("AmountUTari = %d, want 1000", got.AmountUTari)
	}
	if got.Status != invoice.StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, invoice.StatusPending)
	}
	if got.Address == "" {
		t.Error("Address is empty, want the resolved fake address")
	}
	if _, err := uuid.Parse(got.ID); err != nil {
		t.Errorf("ID = %q is not a valid UUID: %v", got.ID, err)
	}
}

func TestPostInvoice_MissingOrderRef(t *testing.T) {
	srv := setupTestServer(t, nil)

	body, _ := json.Marshal(createInvoiceRequest{AmountUTari: 1000})
	resp, err := http.Post(srv.URL+"/invoice", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /invoice: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPostInvoice_ZeroAmount(t *testing.T) {
	srv := setupTestServer(t, nil)

	body, _ := json.Marshal(createInvoiceRequest{OrderRef: "order-x", AmountUTari: 0})
	resp, err := http.Post(srv.URL+"/invoice", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /invoice: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestPostInvoice_WalletAddressResolutionFailure(t *testing.T) {
	srv := setupTestServer(t, errors.New("wallet: connection refused"))

	body, _ := json.Marshal(createInvoiceRequest{OrderRef: "order-fail", AmountUTari: 1000})
	resp, err := http.Post(srv.URL+"/invoice", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /invoice: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}

	var got errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if got.Error == "" {
		t.Error("Error message is empty, want a description of the wallet failure")
	}
}

func TestGetInvoice_Success(t *testing.T) {
	srv := setupTestServer(t, nil)

	body, _ := json.Marshal(createInvoiceRequest{OrderRef: "order-get", AmountUTari: 2500})
	resp, err := http.Post(srv.URL+"/invoice", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /invoice: %v", err)
	}
	var created invoiceResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	resp.Body.Close()

	getResp, err := http.Get(srv.URL + "/invoice/" + created.ID)
	if err != nil {
		t.Fatalf("GET /invoice/%s: %v", created.ID, err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}

	var got invoiceResponse
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("ID = %q, want %q", got.ID, created.ID)
	}
	if got.OrderRef != "order-get" {
		t.Errorf("OrderRef = %q, want %q", got.OrderRef, "order-get")
	}
}

func TestGetInvoice_NotFound(t *testing.T) {
	srv := setupTestServer(t, nil)

	resp, err := http.Get(srv.URL + "/invoice/" + uuid.New().String())
	if err != nil {
		t.Fatalf("GET /invoice/{id}: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestGetInvoice_InvalidID(t *testing.T) {
	srv := setupTestServer(t, nil)

	resp, err := http.Get(srv.URL + "/invoice/not-a-uuid")
	if err != nil {
		t.Fatalf("GET /invoice/{id}: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// ---- registerAdminRoutes tests (S1/I19 admin-auth fix, task brief "fix C2 and add
// auth", part 2) ----

// setupAdminServer connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema,
// and returns a fully-wired *admin.Server backed by fake wallet identify/balance
// funcs and a fake webhook sender — same DI convention as internal/admin's own test
// helper. Skips the calling test if no live Postgres DSN is configured.
func setupAdminServer(t *testing.T) *admin.Server {
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
	webhookStore := webhook.NewStore(pool)
	fakeSender := &fakeWebhookSender{}
	identify := func() (*tari_generated.GetIdentityResponse, error) {
		return &tari_generated.GetIdentityResponse{PublicAddress: "fake-wallet-address"}, nil
	}
	getBalances := func() (*tari_generated.GetBalanceResponse, error) {
		return &tari_generated.GetBalanceResponse{AvailableBalance: 0}, nil
	}

	adminServer, err := admin.New(invoiceStore, webhookStore, fakeSender, identify, getBalances)
	if err != nil {
		t.Fatalf("admin.New() error = %v", err)
	}
	return adminServer
}

// fakeWebhookSender never performs real HTTP requests — these tests only exercise
// route registration/auth-gating, never actual webhook delivery.
type fakeWebhookSender struct{}

func (f *fakeWebhookSender) Send(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	return 200, nil
}

// TestRegisterAdminRoutes_UnsetTokenDoesNotRegisterRoutes confirms that
// registerAdminRoutes, called with an empty authToken (i.e. TARIPAY_ADMIN_AUTH_TOKEN
// unset per config.Config.AdminAuthToken's doc comment), does not register the admin
// routes on the mux AT ALL — a request to /admin must 404 (route not found), NOT 401
// (which would imply the route exists but auth failed). This is the meaningfully
// different, intentional signal the task brief calls for when the operator forgot to
// configure admin auth.
func TestRegisterAdminRoutes_UnsetTokenDoesNotRegisterRoutes(t *testing.T) {
	adminServer := setupAdminServer(t)

	mux := http.NewServeMux()
	registerAdminRoutes(mux, adminServer, "")

	for _, tc := range []struct {
		method string
		target string
	}{
		{http.MethodGet, "/admin"},
		{http.MethodGet, "/admin/invoices"},
		{http.MethodPost, "/admin/webhooks/00000000-0000-0000-0000-000000000000/retry"},
	} {
		req := httptest.NewRequest(tc.method, tc.target, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: status = %d, want %d (route must not be registered at all when AdminAuthToken is unset)", tc.method, tc.target, rec.Code, http.StatusNotFound)
		}
	}
}

// TestRegisterAdminRoutes_SetTokenRegistersProtectedRoutes confirms the opposite:
// when authToken is non-empty, the admin routes ARE registered, and reachable
// (behind auth) — a request with no Authorization header gets 401 (route exists,
// auth required), not 404.
func TestRegisterAdminRoutes_SetTokenRegistersProtectedRoutes(t *testing.T) {
	adminServer := setupAdminServer(t)

	mux := http.NewServeMux()
	registerAdminRoutes(mux, adminServer, "some-admin-token")

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d (route must be registered and auth-gated when AdminAuthToken is set)", rec.Code, http.StatusUnauthorized)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.Header.Set("Authorization", "Bearer some-admin-token")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d with the correct token, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// ---- runWebhookRetryLoop tests (webhook-delivery-reliability fix, task brief part
// 2, item 2) ----

// setupWebhookStore connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema,
// and returns a webhook.Store — sufficient on its own for
// TestRunWebhookRetryLoop_StopsOnContextCancellation below, which never seeds any
// delivery rows (an empty webhook_deliveries table needs no fixture invoice row).
// Skips the calling test if no live Postgres DSN is configured.
func setupWebhookStore(t *testing.T) *webhook.Store {
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

	return webhook.NewStore(pool)
}

// TestRunWebhookRetryLoop_StopsOnContextCancellation confirms runWebhookRetryLoop's
// goroutine is actually wired to, and respects, its shutdown context: it starts
// (ticking on a short interval so it's exercised at least once), and returns promptly
// once ctx is cancelled — same "goroutine started, cancellation respected" shape as
// the existing event-watcher supervisor loop (runEventWatcherWithBackoff), which this
// test's structure mirrors. This deliberately does not test the full production 60s
// webhookRetryTickerInterval — interval is passed in directly, short, so the test
// doesn't need to wait anywhere near that long.
func TestRunWebhookRetryLoop_StopsOnContextCancellation(t *testing.T) {
	webhookStore := setupWebhookStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runWebhookRetryLoop(ctx, webhookStore, &fakeWebhookSender{}, 5*time.Millisecond)
		close(done)
	}()

	// Give it a moment to actually start ticking (an empty webhook_deliveries
	// table means each tick's RetryFailedDeliveries call is a fast, harmless
	// no-op) before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// runWebhookRetryLoop returned promptly after cancellation, as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("runWebhookRetryLoop did not return within 2s of context cancellation")
	}
}

// ---- runTTLSweepLoop tests (TTL enforcement fix, task brief part 2, item 1) ----

// TestRunTTLSweepLoop_StopsOnContextCancellation confirms runTTLSweepLoop's
// goroutine is actually wired to, and respects, its shutdown context — same
// "goroutine started, cancellation respected" shape as
// TestRunWebhookRetryLoop_StopsOnContextCancellation above, which this test's
// structure mirrors per the task brief's explicit precedent.
func TestRunTTLSweepLoop_StopsOnContextCancellation(t *testing.T) {
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

	runCtx, runCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runTTLSweepLoop(runCtx, invoiceStore, 5*time.Millisecond)
		close(done)
	}()

	// Give it a moment to actually start ticking (an empty invoices table means
	// each tick's ExpireStale call is a fast, harmless no-op) before cancelling.
	time.Sleep(20 * time.Millisecond)
	runCancel()

	select {
	case <-done:
		// runTTLSweepLoop returned promptly after cancellation, as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("runTTLSweepLoop did not return within 2s of context cancellation")
	}
}

// ---- checkWebhookConfig tests (S3 fail-open-HMAC-secret fix, task brief part 3)
// ----

// TestCheckWebhookConfig_CallbackURLSetSecretUnsetIsFatal confirms the exact
// combination the review flagged (S3): a webhook callback URL configured WITHOUT an
// HMAC secret must cause a fatal startup error (checkWebhookConfig returns a
// non-nil error, which main() turns into log.Fatalf) — sending webhooks signed with
// an empty, anyone-computable key is worse than sending no signature at all.
func TestCheckWebhookConfig_CallbackURLSetSecretUnsetIsFatal(t *testing.T) {
	err := checkWebhookConfig("https://merchant.example/webhook", "")
	if err == nil {
		t.Fatal("checkWebhookConfig(callbackURL set, secret unset) error = nil, want a non-nil fatal-startup error")
	}
}

// TestCheckWebhookConfig_BothUnsetIsNotFatal confirms the unchanged "warn and skip"
// precedent: both fields unset must NOT be fatal — invoice creation/lookup must keep
// working standalone with webhook delivery simply disabled.
func TestCheckWebhookConfig_BothUnsetIsNotFatal(t *testing.T) {
	if err := checkWebhookConfig("", ""); err != nil {
		t.Errorf("checkWebhookConfig(both unset) error = %v, want nil", err)
	}
}

// TestCheckWebhookConfig_OnlyCallbackURLUnsetIsNotFatal confirms the other
// unchanged "warn and skip" precedent: only the callback URL unset (secret set) must
// NOT be fatal — a secret with no callback URL to sign anything for is harmless.
func TestCheckWebhookConfig_OnlyCallbackURLUnsetIsNotFatal(t *testing.T) {
	if err := checkWebhookConfig("", "some-secret"); err != nil {
		t.Errorf("checkWebhookConfig(callback URL unset, secret set) error = %v, want nil", err)
	}
}

// TestCheckWebhookConfig_BothSetIsNotFatal confirms normal operation (both
// configured) is unaffected by this fix.
func TestCheckWebhookConfig_BothSetIsNotFatal(t *testing.T) {
	if err := checkWebhookConfig("https://merchant.example/webhook", "some-secret"); err != nil {
		t.Errorf("checkWebhookConfig(both set) error = %v, want nil", err)
	}
}
