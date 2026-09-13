package admin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"

	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// setupServer connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema, and
// returns a fully-wired *Server plus its underlying invoice/webhook stores for test
// fixtures. Skips the calling test if no live Postgres DSN is configured — same
// convention as every other package's setup helper in this repo.
func setupServer(t *testing.T, sender webhook.SenderInterface, identify IdentifyFunc, getBalances GetBalancesFunc) (*Server, *invoice.Store, *webhook.Store) {
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

	if identify == nil {
		identify = func() (*tari_generated.GetIdentityResponse, error) {
			return &tari_generated.GetIdentityResponse{PublicAddress: "fake-wallet-address"}, nil
		}
	}
	if getBalances == nil {
		getBalances = func() (*tari_generated.GetBalanceResponse, error) {
			return &tari_generated.GetBalanceResponse{AvailableBalance: 1234}, nil
		}
	}

	srv, err := New(invoiceStore, webhookStore, sender, identify, getBalances)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return srv, invoiceStore, webhookStore
}

// fakeSender never performs real HTTP requests — used throughout this test file per
// the same "don't hit real HTTP" requirement webhook/eventwatcher tests follow.
type fakeSender struct {
	statusCode int
	err        error
	calls      int
}

func (f *fakeSender) Send(ctx context.Context, callbackURL string, payload []byte) (int, error) {
	f.calls++
	return f.statusCode, f.err
}

// testAdminAuthToken is the shared bearer token used throughout this test file's
// RegisterRoutes calls — arbitrary, but non-empty (an empty token is itself covered
// separately, see requireAuth's fail-closed behavior tested in auth_test.go... no,
// actually tested below in this file via TestRequireAuth_*).
const testAdminAuthToken = "test-admin-token-do-not-use-in-prod"

// authedRequest builds an httptest.Request with a valid Authorization: Bearer header
// for testAdminAuthToken, for tests exercising handler behavior rather than the auth
// middleware itself.
func authedRequest(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	req.Header.Set("Authorization", "Bearer "+testAdminAuthToken)
	return req
}

func TestHandleDashboard_RendersExpectedContent(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, invoiceStore, webhookStore := setupServer(t, sender, nil, nil)

	ctx := context.Background()
	inv, err := invoiceStore.Create(ctx, "order-dash", 5000, time.Hour)
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}
	if _, err := webhookStore.Create(ctx, inv.ID, "https://merchant.example/webhook", []byte(`{"event":"payment.seen"}`)); err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	body := rec.Body.String()
	for _, want := range []string{"fake-wallet-address", "order-dash", "1234", inv.PaymentID} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard body missing %q\nbody:\n%s", want, body)
		}
	}
}

func TestHandleDashboard_WalletErrorDegradesGracefully(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	identify := func() (*tari_generated.GetIdentityResponse, error) {
		return nil, errors.New("wallet: connection refused")
	}
	srv, _, _ := setupServer(t, sender, identify, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (wallet error should degrade, not 500)", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "unable to reach wallet") {
		t.Errorf("dashboard body missing wallet error note, body:\n%s", rec.Body.String())
	}
}

// captureLog redirects the standard logger's output to an in-memory buffer for the
// duration of the calling test (restored via t.Cleanup) — used by
// TestHandleDashboard_WalletErrorDoesNotLeakDetail below to assert that the
// degraded-dashboard path still logs the real wallet error detail server-side, per
// the task brief's explicit "assert on the log output in the test, not just the
// response body" requirement.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prevOutput := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() {
		log.SetOutput(prevOutput)
	})
	return &buf
}

// TestHandleDashboard_WalletErrorDoesNotLeakDetail covers the dashboard-
// degradation path's generic-error fix (task brief part 3, I4 security-
// boundaries persona): the rendered page must not contain a fake-but-realistic
// wallet gRPC dial address from the underlying error, while the full detail is
// still logged server-side alongside a correlation id that also appears on the
// rendered page.
func TestHandleDashboard_WalletErrorDoesNotLeakDetail(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	sensitiveDetail := "rpc error: code = Unavailable desc = connection error: dial tcp 10.0.0.5:18143: connect: connection refused"
	identify := func() (*tari_generated.GetIdentityResponse, error) {
		return nil, errors.New(sensitiveDetail)
	}
	srv, _, _ := setupServer(t, sender, identify, nil)
	logBuf := captureLog(t)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (wallet error should degrade, not 500)", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if strings.Contains(body, "10.0.0.5") || strings.Contains(body, "dial tcp") {
		t.Errorf("dashboard body leaks internal wallet gRPC dial detail:\n%s", body)
	}
	if !strings.Contains(body, "unable to reach wallet") {
		t.Errorf("dashboard body missing the generic wallet error note, body:\n%s", body)
	}

	// Extract the correlation id the generic message embeds (format:
	// "correlation_id: <uuid>)") to confirm it also appears in the server log
	// alongside the real error detail.
	const marker = "correlation_id: "
	idx := strings.Index(body, marker)
	if idx == -1 {
		t.Fatalf("dashboard body missing a correlation_id, body:\n%s", body)
	}
	rest := body[idx+len(marker):]
	end := strings.IndexAny(rest, ")<")
	if end == -1 {
		t.Fatalf("could not find end of correlation_id in body:\n%s", body)
	}
	correlationID := rest[:end]

	if !strings.Contains(logBuf.String(), sensitiveDetail) {
		t.Errorf("server log missing the full wallet error detail, log:\n%s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), correlationID) {
		t.Errorf("server log missing the correlation id %q rendered on the dashboard, log:\n%s", correlationID, logBuf.String())
	}
}

func TestHandleInvoices_FiltersByStatus(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, invoiceStore, _ := setupServer(t, sender, nil, nil)
	ctx := context.Background()

	pending, err := invoiceStore.Create(ctx, "order-pending", 100, time.Hour)
	if err != nil {
		t.Fatalf("create pending invoice: %v", err)
	}
	confirmed, err := invoiceStore.Create(ctx, "order-confirmed", 200, time.Hour)
	if err != nil {
		t.Fatalf("create confirmed invoice: %v", err)
	}
	now := time.Now().UTC()
	if err := invoiceStore.UpdateStatus(ctx, confirmed.ID, invoice.StatusConfirmed, &now); err != nil {
		t.Fatalf("update status: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodGet, "/admin/invoices?status=confirmed", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "order-confirmed") {
		t.Errorf("body missing order-confirmed:\n%s", body)
	}
	if strings.Contains(body, "order-pending") {
		t.Errorf("body should NOT contain order-pending when filtered to status=confirmed:\n%s", body)
	}
	_ = pending
}

func TestHandleWebhookRetry_CallsSenderAndUpdatesRecord(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, invoiceStore, webhookStore := setupServer(t, sender, nil, nil)
	ctx := context.Background()

	inv, err := invoiceStore.Create(ctx, "order-retry", 100, time.Hour)
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}
	delivery, err := webhookStore.Create(ctx, inv.ID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	// Seed it as already-failed, as if a prior automatic attempt had failed.
	if err := webhookStore.MarkFailed(ctx, delivery.ID, nil, "initial failure"); err != nil {
		t.Fatalf("seed failed delivery: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodPost, "/admin/webhooks/"+delivery.ID.String()+"/retry", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if sender.calls != 1 {
		t.Errorf("sender.calls = %d, want 1 (retry route must call through to the sender)", sender.calls)
	}

	got, err := webhookStore.GetByID(ctx, delivery.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != webhook.StatusDelivered {
		t.Errorf("Status = %q, want %q after successful retry", got.Status, webhook.StatusDelivered)
	}
	if got.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", got.Attempts)
	}

	if !strings.Contains(rec.Body.String(), "retried successfully") {
		t.Errorf("retry response body missing success flash message:\n%s", rec.Body.String())
	}
}

// ---- static asset tests (task brief part 5, I11/I15: self-host HTMX) ----

// TestHandleHTMXStatic_ReturnsEmbeddedFile confirms GET /admin/static/htmx.min.js
// returns 200 with a JS content-type and a non-empty body — and, per this route's
// documented "not privileged data" call (see RegisterRoutes' doc comment),
// succeeds with NO Authorization header at all, unlike every other /admin* route.
func TestHandleHTMXStatic_ReturnsEmbeddedFile(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	// Deliberately unauthenticated request — httptest.NewRequest, not
	// authedRequest — see this test's doc comment above.
	req := httptest.NewRequest(http.MethodGet, "/admin/static/htmx.min.js", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "javascript") {
		t.Errorf("Content-Type = %q, want it to contain %q", contentType, "javascript")
	}

	body := rec.Body.Bytes()
	if len(body) == 0 {
		t.Fatal("body is empty, want the embedded htmx.min.js contents")
	}
	// A light sanity check that this is actually htmx (not an empty/
	// placeholder stand-in file) without pinning to its exact minified
	// bytes, which would make this test brittle across htmx version bumps.
	if !strings.Contains(string(body), "htmx") {
		t.Errorf("body does not contain the string %q, does not look like htmx.min.js", "htmx")
	}
}

func TestHandleWebhookRetry_UnknownIDReturns404(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := authedRequest(http.MethodPost, "/admin/webhooks/00000000-0000-0000-0000-000000000000/retry", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// ---- auth tests (S1/I19 admin-auth fix, task brief "fix C2 and add auth", part 2)
// ----

// adminRouteTestCases enumerates every admin route this fix must protect, per the
// brief's explicit "confirm the retry route specifically still requires auth" and
// "EACH admin route" instructions.
func adminRouteTestCases() []struct {
	name   string
	method string
	target string
} {
	return []struct {
		name   string
		method string
		target string
	}{
		{"dashboard", http.MethodGet, "/admin"},
		{"invoices list", http.MethodGet, "/admin/invoices"},
		{"webhook retry", http.MethodPost, "/admin/webhooks/00000000-0000-0000-0000-000000000000/retry"},
	}
}

// TestRequireAuth_MissingHeaderReturns401 covers every admin route with no
// Authorization header at all.
func TestRequireAuth_MissingHeaderReturns401(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	for _, tc := range adminRouteTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d (no Authorization header)", rec.Code, http.StatusUnauthorized)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("WWW-Authenticate header missing on 401 response")
			}
		})
	}
}

// TestRequireAuth_WrongTokenReturns401 covers every admin route with a
// well-formed but incorrect bearer token.
func TestRequireAuth_WrongTokenReturns401(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	for _, tc := range adminRouteTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			req.Header.Set("Authorization", "Bearer wrong-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d (wrong token)", rec.Code, http.StatusUnauthorized)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("WWW-Authenticate header missing on 401 response")
			}
		})
	}
}

// TestRequireAuth_CorrectTokenReachesHandler covers every admin route with the
// correct bearer token, confirming the request reaches the real handler (i.e. never
// a 401) — for the retry route specifically this also confirms the sender was
// actually invoked, not just that some 2xx/404 status was returned.
func TestRequireAuth_CorrectTokenReachesHandler(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, invoiceStore, webhookStore := setupServer(t, sender, nil, nil)
	ctx := context.Background()

	inv, err := invoiceStore.Create(ctx, "order-auth-retry", 100, time.Hour)
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}
	delivery, err := webhookStore.Create(ctx, inv.ID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	if err := webhookStore.MarkFailed(ctx, delivery.ID, nil, "initial failure"); err != nil {
		t.Fatalf("seed failed delivery: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	for _, tc := range []struct {
		name       string
		method     string
		target     string
		wantStatus int
	}{
		{"dashboard", http.MethodGet, "/admin", http.StatusOK},
		{"invoices list", http.MethodGet, "/admin/invoices", http.StatusOK},
		{"webhook retry", http.MethodPost, "/admin/webhooks/" + delivery.ID.String() + "/retry", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := authedRequest(tc.method, tc.target, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (correct token must reach the real handler), body: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}

	if sender.calls != 1 {
		t.Errorf("sender.calls = %d, want 1 (retry route with correct token must call through to the sender)", sender.calls)
	}
}

// TestRequireAuth_RetryRouteSpecificallyRequiresAuth is a dedicated, narrowly-scoped
// regression test for the retry POST route per the brief's explicit callout: "this
// was the one people most likely to bypass in a quick fix". Verifies both that an
// unauthenticated retry attempt is rejected AND that it never reaches the sender
// (i.e. the auth check happens before any side effect, not just before the response
// is written).
func TestRequireAuth_RetryRouteSpecificallyRequiresAuth(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, invoiceStore, webhookStore := setupServer(t, sender, nil, nil)
	ctx := context.Background()

	inv, err := invoiceStore.Create(ctx, "order-retry-noauth", 100, time.Hour)
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}
	delivery, err := webhookStore.Create(ctx, inv.ID, "https://merchant.example/webhook", []byte(`{}`))
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	if err := webhookStore.MarkFailed(ctx, delivery.ID, nil, "initial failure"); err != nil {
		t.Fatalf("seed failed delivery: %v", err)
	}

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, testAdminAuthToken)

	req := httptest.NewRequest(http.MethodPost, "/admin/webhooks/"+delivery.ID.String()+"/retry", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d (retry route without auth must be rejected)", rec.Code, http.StatusUnauthorized)
	}
	if sender.calls != 0 {
		t.Errorf("sender.calls = %d, want 0 (retry must not reach the sender without valid auth)", sender.calls)
	}

	got, err := webhookStore.GetByID(ctx, delivery.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0 (retry without auth must not increment attempts)", got.Attempts)
	}
}

// TestRequireAuth_EmptyTokenAlwaysRejects covers requireAuth's fail-closed behavior:
// an empty configured token must reject every request, even one with an empty
// Authorization header/token, rather than treating "" == "" as a match. This is
// defense-in-depth for a caller bug — cmd/gateway/main.go is expected to never call
// RegisterRoutes with an empty token in the first place (see that function's own
// "don't register admin routes at all if unset" logic), but this test ensures the
// middleware itself doesn't silently open up if that invariant is ever violated.
func TestRequireAuth_EmptyTokenAlwaysRejects(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux, "")

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.Header.Set("Authorization", "Bearer ")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (empty configured token must always reject)", rec.Code, http.StatusUnauthorized)
	}
}
