package admin

import (
	"context"
	"errors"
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
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
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
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (wallet error should degrade, not 500)", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "unable to reach wallet") {
		t.Errorf("dashboard body missing wallet error note, body:\n%s", rec.Body.String())
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
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/admin/invoices?status=confirmed", nil)
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
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/admin/webhooks/"+delivery.ID.String()+"/retry", nil)
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

func TestHandleWebhookRetry_UnknownIDReturns404(t *testing.T) {
	sender := &fakeSender{statusCode: 200}
	srv, _, _ := setupServer(t, sender, nil, nil)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/admin/webhooks/00000000-0000-0000-0000-000000000000/retry", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
