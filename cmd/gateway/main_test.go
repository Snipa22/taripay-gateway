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

	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
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
