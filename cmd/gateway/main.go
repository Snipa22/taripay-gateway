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

// webhookRetryTickerInterval is how often runWebhookRetryLoop below calls
// webhook.RetryFailedDeliveries (task brief part 2, item 2). 60s is a PLACEHOLDER,
// UNCONFIRMED value — same "reasonable-sounding guess, not project-owner-approved"
// caveat as eventWatcherInitialBackoff/eventWatcherMaxBackoff above and
// webhook.DefaultRetryMaxAge/DefaultRetryMaxAttempts.
const webhookRetryTickerInterval = 60 * time.Second

// ttlSweepTickerInterval is how often runTTLSweepLoop below calls
// invoice.Store.ExpireStale (task brief part 2, item 1 — TTL enforcement). 60s is a
// PLACEHOLDER, UNCONFIRMED value, same "reasonable-sounding guess, not
// project-owner-approved" caveat as webhookRetryTickerInterval above and
// config.DefaultInvoiceTTLMinutes: the invoice TTL itself is measured in minutes, so
// a 60s sweep interval means an expired invoice is caught within, at worst, roughly
// one sweep interval of its expires_at passing — the confirm-time check added
// alongside this sweep (see internal/eventwatcher's handleTransaction) closes the
// race this interval alone can't (a payment landing in the exact window between
// expires_at passing and the next sweep tick).
const ttlSweepTickerInterval = 60 * time.Second

func main() {
	configFile := flag.String("config", "", "Path to a TOML config file (env: TARIPAY_CONFIG_FILE)")
	walletGRPCAddr := flag.String("wallet-grpc-addr", "", "minotari_console_wallet gRPC address (env: TARIPAY_WALLET_GRPC_ADDR; default: "+config.DefaultWalletGRPCAddress+")")
	postgresDSN := flag.String("postgres-dsn", "", "Postgres connection string (env: TARIPAY_POSTGRES_DSN; required, no default)")
	httpAddr := flag.String("http-addr", "", "HTTP listen address (env: TARIPAY_HTTP_LISTEN_ADDR; default: "+config.DefaultHTTPListenAddr+")")
	webhookCallbackURL := flag.String("webhook-callback-url", "", "Merchant webhook callback URL (env: TARIPAY_WEBHOOK_CALLBACK_URL; no default, webhook delivery disabled if unset)")
	webhookHMACSecret := flag.String("webhook-hmac-secret", "", "Webhook HMAC signing secret (env: TARIPAY_WEBHOOK_HMAC_SECRET; no default, webhook delivery disabled if unset)")
	adminAuthToken := flag.String("admin-auth-token", "", "Shared bearer token required on all /admin* routes (env: TARIPAY_ADMIN_AUTH_TOKEN; no default, admin routes are NOT registered at all if unset)")
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
		AdminAuthToken:     *adminAuthToken,
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

	// Unlike the webhook fields above, an unset admin auth token is NOT just a
	// "warn and keep going" situation (S1/I19 admin-auth fix, task brief "fix
	// C2 and add auth", part 2): the admin surface (wallet balance, every
	// order, webhook-delivery replay) is a real, operator-only attack surface
	// on an all-interfaces listener, and running it unauthenticated because the
	// operator forgot to set one env var is exactly the finding this fix
	// closes. So this is louder than a WARNING, and — critically — the admin
	// routes are simply never registered on the mux at all below (see
	// registerAdminRoutes), so /admin* 404s rather than silently serving
	// unauthenticated.
	if cfg.AdminAuthToken == "" {
		log.Printf("gateway: WARNING: no admin auth token configured (TARIPAY_ADMIN_AUTH_TOKEN) — admin routes (/admin, /admin/invoices, /admin/webhooks/*/retry) will NOT be registered at all until this is set")
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
	go runEventWatcherWithBackoff(ctx, watcher, walletGRPC.GetCompletedTransactionsByPaymentID)

	// TTL enforcement's periodic sweep half (task brief part 2, item 1) — the
	// other half, the confirm-time check, lives in
	// internal/eventwatcher.handleTransaction (shared by both the live stream and
	// Reconcile). Started alongside the event-watcher supervisor loop above,
	// stopped cleanly on the same shutdown ctx — same shape as
	// runWebhookRetryLoop below.
	go runTTLSweepLoop(ctx, invoiceStore, ttlSweepTickerInterval)

	// Automatic background retry loop for failed/stuck webhook deliveries (task
	// brief part 2, item 2). Started alongside the event-watcher supervisor loop
	// above, stopped cleanly on the same shutdown ctx — see runWebhookRetryLoop's
	// doc comment for how this composes with (and does not replace) the admin
	// UI's manual retry button.
	go runWebhookRetryLoop(ctx, webhookStore, sender, webhookRetryTickerInterval)

	adminServer, err := admin.New(invoiceStore, webhookStore, sender, walletGRPC.Identify, walletGRPC.GetBalances)
	if err != nil {
		log.Fatalf("gateway: admin: %v", err)
	}

	handler := newHandler(invoiceStore, time.Duration(cfg.InvoiceTTLMinutes)*time.Minute)
	registerAdminRoutes(handler, adminServer, cfg.AdminAuthToken)

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

// registerAdminRoutes registers adminServer's routes onto mux, gated on authToken
// being configured (the S1/I19 admin-auth fix, task brief "fix C2 and add auth",
// part 2): if authToken is empty, this deliberately does NOT call
// adminServer.RegisterRoutes at all, so /admin* 404s (route not found) rather than
// running unauthenticated or returning a merely-confusing 401 for every request —
// a meaningfully different, more diagnostic signal that the operator forgot to
// configure TARIPAY_ADMIN_AUTH_TOKEN, per the brief's explicit requirement. The
// startup warning for this same condition is logged once in main() above; this
// function only performs the actual (non-)registration.
func registerAdminRoutes(mux *http.ServeMux, adminServer *admin.Server, authToken string) {
	if authToken == "" {
		return
	}
	adminServer.RegisterRoutes(mux, authToken)
}

// runEventWatcherWithBackoff supervises watcher.Run with restart-with-backoff on
// error: Run's own doc comment explicitly delegates this responsibility to its
// caller. Backoff resets to eventWatcherInitialBackoff after every restart attempt
// (whether or not it succeeds) — simplest possible policy for v1; a smarter one (only
// resetting after a sustained period of successful running) is not worth the
// complexity here, since a wallet gRPC connection dropping repeatedly in a tight loop
// is itself a signal worth surfacing loudly via the resulting log spam, not silently
// smoothing over.
//
// Also runs watcher.Reconcile (the reconciliation-after-downtime fix, task brief
// part 1, S2/I7) before every attempt to (re)open the live stream below — both the
// very first attempt (gateway startup) and every restart-with-backoff reconnect
// after a stream error, since that reconnect window is exactly the downtime this
// closes. Reconcile runs SYNCHRONOUSLY here (blocking, before watcher.Run is
// called) rather than concurrently with the live stream — a deliberate
// simplification over the task brief's "your call" allowance for a concurrent
// variant: running it synchronously avoids the double-count/race concern entirely
// (no live event and no reconciled historical transaction can ever be "in flight"
// for the same invoice at the same time), rather than relying on the
// already-merged AddReceivedAmount atomic-accumulate fix to merely make such a race
// harmless. Reconciling a handful of non-terminal invoices via one gRPC call each
// is expected to be fast relative to how rarely this runs (gateway startup, and
// stream-error reconnects), so the added latency before the live stream (re)opens
// is not a practical concern.
func runEventWatcherWithBackoff(ctx context.Context, watcher *eventwatcher.Watcher, getCompletedByPaymentID eventwatcher.GetCompletedByPaymentIDFunc) {
	backoff := eventWatcherInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		if n, err := watcher.Reconcile(ctx, getCompletedByPaymentID); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("gateway: eventwatcher: reconcile: %v", err)
		} else {
			log.Printf("gateway: eventwatcher: reconcile: examined %d non-terminal invoice(s)", n)
		}
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

// runWebhookRetryLoop is the periodic automatic retry loop for failed/stuck webhook
// deliveries (task brief part 2, item 2): every interval, it calls
// webhook.RetryFailedDeliveries once (using webhook.DefaultRetryMaxAge as the
// eligibility window — see that constant's doc comment), logs the outcome, and
// repeats, until ctx is cancelled. Checking ctx.Done() in the select below (rather
// than only ever waiting on the ticker) means a context that's already cancelled by
// the time this runs returns promptly without waiting for a full tick interval first.
//
// This does NOT replace the admin UI's manual retry button (internal/admin's
// handleWebhookRetry) — that stays available for an operator who wants to force an
// immediate retry outside this periodic cycle. Both paths share the exact same
// underlying webhook.Attempt call (via webhook.RetryFailedDeliveries here,
// internal/admin's retryAndDescribe there) — neither duplicates the
// increment/send/mark sequence.
func runWebhookRetryLoop(ctx context.Context, webhookStore *webhook.Store, sender webhook.SenderInterface, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := webhook.RetryFailedDeliveries(ctx, webhookStore, sender, webhook.DefaultRetryMaxAge)
			if err != nil {
				log.Printf("gateway: webhook retry loop: %v", err)
				continue
			}
			if count > 0 {
				log.Printf("gateway: webhook retry loop: retried %d delivery(ies)", count)
			}
		}
	}
}

// runTTLSweepLoop is the periodic half of TTL enforcement (task brief part 2, item
// 1): every interval, it calls invoice.Store.ExpireStale once (moving any
// pending/seen invoice whose expires_at has passed to StatusExpired), logs the
// outcome, and repeats, until ctx is cancelled. Same shape as runWebhookRetryLoop
// above (ticker, select on ctx.Done()/ticker.C) — checking ctx.Done() in the select
// (rather than only ever waiting on the ticker) means a context that's already
// cancelled by the time this runs returns promptly without waiting for a full tick
// interval first.
//
// This is only HALF of TTL enforcement — the other half, the confirm-time check
// that closes the race this sweep alone can still lose (a payment landing in the
// exact window between an invoice's expires_at passing and the next sweep tick),
// lives in internal/eventwatcher.handleTransaction, shared by both the live event
// stream and Reconcile.
func runTTLSweepLoop(ctx context.Context, invoiceStore *invoice.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := invoiceStore.ExpireStale(ctx)
			if err != nil {
				log.Printf("gateway: ttl sweep loop: %v", err)
				continue
			}
			if count > 0 {
				log.Printf("gateway: ttl sweep loop: expired %d invoice(s)", count)
			}
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
