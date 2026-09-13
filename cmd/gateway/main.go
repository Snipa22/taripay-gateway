// Command gateway is TariPay Gateway's core HTTP bootstrap for Phase 1a: invoice
// creation, wallet address resolution, and Postgres persistence.
//
// Phase 1b (a separate, later dispatch) adds the event-watcher/webhook delivery loop
// and the HTMX admin UI on top of what this binary starts up — neither exists yet, by
// design (see AGENTS.md / the task brief's explicit scope constraints).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-lib/walletGRPC"

	"github.com/Snipa22/taripay-gateway/internal/config"
	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
)

func main() {
	configFile := flag.String("config", "", "Path to a TOML config file (env: TARIPAY_CONFIG_FILE)")
	walletGRPCAddr := flag.String("wallet-grpc-addr", "", "minotari_console_wallet gRPC address (env: TARIPAY_WALLET_GRPC_ADDR; default: "+config.DefaultWalletGRPCAddress+")")
	postgresDSN := flag.String("postgres-dsn", "", "Postgres connection string (env: TARIPAY_POSTGRES_DSN; required, no default)")
	httpAddr := flag.String("http-addr", "", "HTTP listen address (env: TARIPAY_HTTP_LISTEN_ADDR; default: "+config.DefaultHTTPListenAddr+")")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(config.Flags{
		ConfigFile:        *configFile,
		WalletGRPCAddress: *walletGRPCAddr,
		PostgresDSN:       *postgresDSN,
		HTTPListenAddr:    *httpAddr,
	})
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}

	// InitWalletGRPC establishes the wallet gRPC connection go-tari-lib's walletGRPC
	// package uses internally for every subsequent call in this process — it must be
	// called exactly once, before any other walletGRPC function, per that package's
	// own contract.
	walletGRPC.InitWalletGRPC(cfg.WalletGRPCAddress)

	database, err := db.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}
	defer database.Close()

	if err := database.Migrate(ctx); err != nil {
		log.Fatalf("gateway: migrate: %v", err)
	}

	// Wire the real walletGRPC.GetPaymentIdAddress as invoice.Store's address
	// resolver, adapting its richer response down to the single base58 address
	// string this repo persists. We use InteractiveAddressBase58 rather than the
	// one-sided variant: an invoice payment is expected to be sent while this
	// merchant's own wallet daemon is online to receive it (that's the whole premise
	// of a self-hosted, always-running gateway per AGENTS.md's deployment model), so
	// there is no reason to prefer the one-sided-only address form here.
	resolveAddress := func(paymentID string) (string, error) {
		resp, err := walletGRPC.GetPaymentIdAddress(paymentID)
		if err != nil {
			return "", fmt.Errorf("walletGRPC.GetPaymentIdAddress: %w", err)
		}
		if resp.GetInteractiveAddressBase58() == "" {
			return "", fmt.Errorf("walletGRPC.GetPaymentIdAddress: empty interactive address for payment_id %s", paymentID)
		}
		return resp.GetInteractiveAddressBase58(), nil
	}

	store := invoice.NewStore(database.Pool, resolveAddress)

	handler := newHandler(store, time.Duration(cfg.InvoiceTTLMinutes)*time.Minute)

	log.Printf("gateway: listening on %s (wallet grpc: %s)", cfg.HTTPListenAddr, cfg.WalletGRPCAddress)
	httpServer := &http.Server{
		Addr:    cfg.HTTPListenAddr,
		Handler: handler,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gateway: %v", err)
	}
	log.Printf("gateway: shutting down")
}

// createInvoiceRequest is the JSON body accepted by POST /invoice.
type createInvoiceRequest struct {
	OrderRef    string `json:"order_ref"`
	AmountUTari uint64 `json:"amount_utari"`
}

// invoiceResponse is the JSON shape returned for a single invoice, by both POST
// /invoice and GET /invoice/{id}.
type invoiceResponse struct {
	ID          string  `json:"id"`
	PaymentID   string  `json:"payment_id"`
	OrderRef    string  `json:"order_ref"`
	AmountUTari uint64  `json:"amount_utari"`
	Address     string  `json:"address"`
	Status      string  `json:"status"`
	CreatedAt   string  `json:"created_at"`
	ExpiresAt   string  `json:"expires_at"`
	ConfirmedAt *string `json:"confirmed_at,omitempty"`
}

func toInvoiceResponse(inv *invoice.Invoice) invoiceResponse {
	resp := invoiceResponse{
		ID:          inv.ID.String(),
		PaymentID:   inv.PaymentID,
		OrderRef:    inv.OrderRef,
		AmountUTari: inv.AmountUTari,
		Address:     inv.Address,
		Status:      inv.Status,
		CreatedAt:   inv.CreatedAt.Format(time.RFC3339),
		ExpiresAt:   inv.ExpiresAt.Format(time.RFC3339),
	}
	if inv.ConfirmedAt != nil {
		s := inv.ConfirmedAt.Format(time.RFC3339)
		resp.ConfirmedAt = &s
	}
	return resp
}

// errorResponse is the JSON shape returned for any 4xx/5xx error.
type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// newHandler builds the Phase 1a HTTP surface: POST /invoice (create) and GET
// /invoice/{id} (fetch current state). No admin UI, no webhook delivery — both
// explicitly Phase 1b, per the task brief.
func newHandler(store *invoice.Store, defaultTTL time.Duration) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /invoice", func(w http.ResponseWriter, r *http.Request) {
		var req createInvoiceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		if req.OrderRef == "" {
			writeError(w, http.StatusBadRequest, "order_ref is required")
			return
		}
		if req.AmountUTari == 0 {
			writeError(w, http.StatusBadRequest, "amount_utari must be greater than 0")
			return
		}

		inv, err := store.Create(r.Context(), req.OrderRef, req.AmountUTari, defaultTTL)
		if err != nil {
			// Wallet address resolution failure (or any other Create failure) is
			// reported as a 502: the request was well-formed, but this gateway
			// could not complete it because an upstream dependency (the wallet)
			// failed.
			writeError(w, http.StatusBadGateway, "failed to create invoice: "+err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, toInvoiceResponse(inv))
	})

	mux.HandleFunc("GET /invoice/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid invoice id: "+err.Error())
			return
		}

		inv, err := store.GetByID(r.Context(), id)
		if err != nil {
			if errors.Is(err, invoice.ErrNotFound) {
				writeError(w, http.StatusNotFound, "invoice not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to fetch invoice: "+err.Error())
			return
		}

		writeJSON(w, http.StatusOK, toInvoiceResponse(inv))
	})

	return mux
}
