// Command gateway is TariPay Gateway's HTTP bootstrap: invoice creation, wallet
// address resolution, Postgres persistence (Phase 1a), plus Phase 1b's event-watcher/
// webhook delivery loop and HTMX admin UI, all wired up here.
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

	"github.com/Snipa22/taripay-gateway/internal/admin"
	"github.com/Snipa22/taripay-gateway/internal/config"
	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/eventwatcher"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// eventWatcherInitialBackoff/eventWatcherMaxBackoff bound the restart-with-backoff
// loop around Watcher.Run below: on a stream error, wait, then retry, doubling the
// wait each time up to the cap. 30s is a PLACEHOLDER, UNCONFIRMED max — same
// "reasonable-sounding guess, not project-owner-approved" caveat as
// config.DefaultConfirmationDepth/DefaultInvoiceTTLMinutes.
const (
	eventWatcherInitialBackoff = 1 * time.Second
	eventWatcherMaxBackoff     = 30 * time.Second
)

func main() {
	configFile := flag.String("config", "", "Path to a TOML config file (env: TARIPAY_CONFIG_FILE)")
	walletGRPCAddr := flag.String("wallet-grpc-addr", "", "minotari_console_wallet gRPC address (env: TARIPAY_WALLET_GRPC_ADDR; default: "+config.DefaultWalletGRPCAddress+")")
	postgresDSN := flag.String("postgres-dsn", "", "Postgres connection string (env: TARIPAY_POSTGRES_DSN; required, no default)")
	httpAddr := flag.String("http-addr", "", "HTTP listen address (env: TARIPAY_HTTP_LISTEN_ADDR; default: "+config.DefaultHTTPListenAddr+")")
	webhookCallbackURL := flag.String("webhook-callback-url", "", "Merchant webhook callback URL (env: TARIPAY_WEBHOOK_CALLBACK_URL; no default, webhook delivery disabled if unset)")
	webhookHMACSecret := flag.String("webhook-hmac-secret", "", "Webhook HMAC signing secret (env: TARIPAY_WEBHOOK_HMAC_SECRET; no default, webhook delivery disabled if unset)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(config.Flags{
		ConfigFile:         *configFile,
		WalletGRPCAddress:  *walletGRPCAddr,
		PostgresDSN:        *postgresDSN,
		HTTPListenAddr:     *httpAddr,
		WebhookCallbackURL: *webhookCallbackURL,
		WebhookHMACSecret:  *webhookHMACSecret,
	})
	if err != nil {
		log.Fatalf("gateway: %v", err)
	}

	// Per the task brief: an unset webhook callback URL/HMAC secret is NOT fatal —
	// invoice creation/lookup must keep working standalone — but it IS worth a
	// loud warning, since it silently means every invoice-status transition's
	// webhook delivery will be skipped (see internal/eventwatcher.fireWebhook).
	if cfg.WebhookCallbackURL == "" {
		log.Printf("gateway: WARNING: no webhook callback URL configured (TARIPAY_WEBHOOK_CALLBACK_URL) — webhook delivery is disabled")
	}
	if cfg.WebhookHMACSecret == "" {
		log.Printf("gateway: WARNING: no webhook HMAC secret configured (TARIPAY_WEBHOOK_HMAC_SECRET) — outgoing webhooks (if any) will be signed with an empty secret")
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

	invoiceStore := invoice.NewStore(database.Pool, resolveAddress)
	webhookStore := webhook.NewStore(database.Pool)
	sender := webhook.NewSender(cfg.WebhookHMACSecret)

	watcher := eventwatcher.NewWatcher(invoiceStore, webhookStore, sender, cfg.WebhookCallbackURL, walletGRPC.StreamTransactionEvents)
	go runEventWatcherWithBackoff(ctx, watcher)

	adminServer, err := admin.New(invoiceStore, webhookStore, sender, walletGRPC.Identify, walletGRPC.GetBalances)
	if err != nil {
		log.Fatalf("gateway: admin: %v", err)
	}

	handler := newHandler(invoiceStore, time.Duration(cfg.InvoiceTTLMinutes)*time.Minute)
	adminServer.RegisterRoutes(handler)

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

// runEventWatcherWithBackoff supervises watcher.Run with restart-with-backoff on
// error: Run's own doc comment explicitly delegates this responsibility to its
// caller. Backoff resets to eventWatcherInitialBackoff after every restart attempt
// (whether or not it succeeds) — simplest possible policy for v1; a smarter one (only
// resetting after a sustained period of successful running) is not worth the
// complexity here, since a wallet gRPC connection dropping repeatedly in a tight loop
// is itself a signal worth surfacing loudly via the resulting log spam, not silently
// smoothing over.
func runEventWatcherWithBackoff(ctx context.Context, watcher *eventwatcher.Watcher) {
	backoff := eventWatcherInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		err := watcher.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("gateway: eventwatcher: %v (restarting in %s)", err, backoff)
		} else {
			log.Printf("gateway: eventwatcher: stream ended cleanly (restarting in %s)", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > eventWatcherMaxBackoff {
			backoff = eventWatcherMaxBackoff
		}
	}
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
// /invoice/{id} (fetch current state). Returns the concrete *http.ServeMux (rather
// than the http.Handler interface) so main() can register Phase 1b's admin routes
// onto the same mux afterwards via adminServer.RegisterRoutes — see that method's own
// doc comment for why routes are composed this way instead of each package owning its
// own top-level handler.
func newHandler(store *invoice.Store, defaultTTL time.Duration) *http.ServeMux {
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
