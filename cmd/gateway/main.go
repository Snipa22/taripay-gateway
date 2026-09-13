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
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
	"github.com/Snipa22/go-tari-lib/walletGRPC"

	"github.com/Snipa22/taripay-gateway/internal/admin"
	"github.com/Snipa22/taripay-gateway/internal/config"
	"github.com/Snipa22/taripay-gateway/internal/db"
	"github.com/Snipa22/taripay-gateway/internal/eventwatcher"
	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// version is this binary's version stamp — the task brief part 4 (I13/I27)
// version-stamp fix. Overridden at build time via
// `-ldflags "-X main.version=<value>"` (see docker/gateway/Dockerfile's
// `go build` invocation); left as "dev" for local/unstamped builds (e.g. `go run`,
// `go test`, or a manual `go build` with no ldflags override). Printed at startup
// and exposed via GET /health's response body below.
var version = "dev"

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

// maxInvoiceBodyBytes caps POST /invoice's request body via http.MaxBytesReader
// (task brief part 2, security-boundaries persona I5): this is an unauthenticated,
// all-interfaces listener, so an unbounded request body is a trivial memory-
// exhaustion vector. 64 KiB is a PLACEHOLDER, UNCONFIRMED value — same
// "reasonable-sounding guess, not project-owner-approved" caveat as
// eventWatcherMaxBackoff/webhookRetryTickerInterval above: a createInvoiceRequest
// body is two small string/int fields and will never legitimately approach this
// size, so 64 KiB leaves generous headroom while still bounding the worst case.
const maxInvoiceBodyBytes = 64 * 1024

// httpServer{Read,ReadHeader,Write,Idle}Timeout bound how long the unauthenticated
// http.Server below will wait on a slow/stalled client at each stage of a request
// (task brief part 2, security-boundaries persona I5) — an unbounded default
// timeout on an all-interfaces listener is a slow-client resource-exhaustion
// vector (a client that opens a connection and trickles bytes, or never sends a
// body, ties up a server goroutine indefinitely). All four are PLACEHOLDER,
// UNCONFIRMED values, same caveat as maxInvoiceBodyBytes above — "reasonable
// defaults for a small JSON API", not load-tested or project-owner-approved.
const (
	httpServerReadHeaderTimeout = 10 * time.Second
	httpServerReadTimeout       = 30 * time.Second
	httpServerWriteTimeout      = 30 * time.Second
	httpServerIdleTimeout       = 60 * time.Second
)

func main() {
	configFile := flag.String("config", "", "Path to a TOML config file (env: TARIPAY_CONFIG_FILE)")
	walletGRPCAddr := flag.String("wallet-grpc-addr", "", "minotari_console_wallet gRPC address (env: TARIPAY_WALLET_GRPC_ADDR; default: "+config.DefaultWalletGRPCAddress+")")
	postgresDSN := flag.String("postgres-dsn", "", "Postgres connection string (env: TARIPAY_POSTGRES_DSN; required, no default)")
	httpAddr := flag.String("http-addr", "", "HTTP listen address (env: TARIPAY_HTTP_LISTEN_ADDR; default: "+config.DefaultHTTPListenAddr+")")
	webhookCallbackURL := flag.String("webhook-callback-url", "", "Merchant webhook callback URL (env: TARIPAY_WEBHOOK_CALLBACK_URL; no default, webhook delivery disabled if unset)")
	webhookHMACSecret := flag.String("webhook-hmac-secret", "", "Webhook HMAC signing secret (env: TARIPAY_WEBHOOK_HMAC_SECRET; no default; required if webhook-callback-url is set — startup fails otherwise)")
	adminAuthToken := flag.String("admin-auth-token", "", "Shared bearer token required on all /admin* routes (env: TARIPAY_ADMIN_AUTH_TOKEN; no default, admin routes are NOT registered at all if unset)")
	printVersion := flag.Bool("version", false, "Print the gateway version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println(version)
		return
	}

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

	if err := checkWebhookConfig(cfg.WebhookCallbackURL, cfg.WebhookHMACSecret); err != nil {
		log.Fatalf("gateway: %v", err)
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

	handler := newHandler(invoiceStore, time.Duration(cfg.InvoiceTTLMinutes)*time.Minute, database.Pool, walletGRPC.GetWalletConnectivity, version)
	registerAdminRoutes(handler, adminServer, cfg.AdminAuthToken)

	log.Printf("gateway: version %s, listening on %s (wallet grpc: %s)", version, cfg.HTTPListenAddr, cfg.WalletGRPCAddress)
	httpServer := &http.Server{
		Addr:              cfg.HTTPListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: httpServerReadHeaderTimeout,
		ReadTimeout:       httpServerReadTimeout,
		WriteTimeout:      httpServerWriteTimeout,
		IdleTimeout:       httpServerIdleTimeout,
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

// checkWebhookConfig resolves the combined webhook-callback-URL/HMAC-secret startup
// check (the S3 fail-open-HMAC-secret fix, task brief part 3): the two fields used
// to be validated independently (each just its own "warn and keep going" if unset),
// which meant a callback URL configured WITHOUT an HMAC secret still built a working
// webhook.Sender and sent real webhooks — signed with an empty key anyone can
// compute, i.e. a "valid" signature that proves nothing. This function is the one
// place that now decides all four combinations:
//
//   - both unset: warn and skip (unchanged from before — webhook delivery is simply
//     disabled, invoice creation/lookup keeps working standalone). Returns nil.
//   - callback URL unset, secret set: warn and skip (unchanged) — a secret with no
//     callback URL to sign anything for is harmless, just pointless configuration.
//     Returns nil.
//   - callback URL set, secret unset: FATAL. Per the review's exact framing (cited
//     here, not re-derived): "no callback URL => no webhooks; a callback URL with a
//     computable empty-key signature is worse than no signature at all." Returns a
//     non-nil error describing this; main() below turns that into a log.Fatalf at
//     startup — simpler to reason about than trying to suppress sending at request
//     time — rather than a runtime behavior change deep inside
//     internal/eventwatcher/internal/webhook.
//   - both set: normal operation, nothing to log. Returns nil.
//
// Returning an error here (rather than calling log.Fatalf directly) keeps this
// function itself trivially unit-testable — see TestCheckWebhookConfig_* in
// main_test.go — without needing a subprocess harness to observe an os.Exit.
func checkWebhookConfig(callbackURL, hmacSecret string) error {
	switch {
	case callbackURL == "" && hmacSecret == "":
		log.Printf("gateway: WARNING: no webhook callback URL or HMAC secret configured (TARIPAY_WEBHOOK_CALLBACK_URL / TARIPAY_WEBHOOK_HMAC_SECRET) — webhook delivery is disabled")
		return nil
	case callbackURL == "" && hmacSecret != "":
		log.Printf("gateway: WARNING: no webhook callback URL configured (TARIPAY_WEBHOOK_CALLBACK_URL) — webhook delivery is disabled (the configured HMAC secret is unused)")
		return nil
	case callbackURL != "" && hmacSecret == "":
		return fmt.Errorf("webhook callback URL (TARIPAY_WEBHOOK_CALLBACK_URL=%q) is configured but no webhook HMAC secret (TARIPAY_WEBHOOK_HMAC_SECRET) is set — refusing to start: sending webhooks signed with an empty key produces a \"valid\" signature that proves nothing, which is worse than sending no signature at all. Set TARIPAY_WEBHOOK_HMAC_SECRET, or unset the callback URL to disable webhook delivery entirely", callbackURL)
	default:
		// Both set: normal operation.
		return nil
	}
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
//
// AmountUTari is decoded as uint64 (matching go-tari-lib's own convention, and this
// package's public invoiceResponse shape) rather than int64, specifically so the
// explicit math.MaxInt64 upper-bound check in the POST /invoice handler below can
// catch an oversized value BEFORE it's ever cast to int64 for storage — decoding
// straight into int64 would let json.Decoder itself silently produce a negative
// number for any input above math.MaxInt64 (wraparound), which is exactly the
// silent-negative-BIGINT bug this check (task brief part 2, security-boundaries
// persona I5) closes.
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
//
// CorrelationID is only ever populated for the 502/500 "something failed on our
// end" paths (task brief part 3, I4 security-boundaries persona) — the generic
// message it accompanies deliberately omits the real error's text (a wallet gRPC
// dial address, pgx/SQL error detail, etc.) since this is an unauthenticated API
// response, but an operator investigating a bug report can grep the server log for
// this exact ID to find the real error logAndCorrelate recorded server-side. 4xx
// client-input-validation errors (bad JSON, missing/invalid fields) do not set
// this — those messages describe the CLIENT's mistake, not an internal failure,
// and were never the leak this fix closes.
type errorResponse struct {
	Error         string `json:"error"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// logAndCorrelate logs the full detail of a server-side failure (context describes
// where it happened, err is the real error — a wallet gRPC dial address, a pgx/SQL
// error, etc.) alongside a freshly generated correlation ID, and returns that ID.
// Callers embed the returned ID in a generic, detail-free message sent to the
// client (see errorResponse's doc comment) — this is the link an operator uses to
// find the matching full-detail log line from a user's bug report.
//
// This is task brief part 3's (I4 security-boundaries persona) fix: internal error
// text must never reach an unauthenticated API client verbatim. A per-request UUID
// generated inline is deliberately used instead of a full request-ID middleware
// framework — checked first: neither net/http's httputil nor any existing code in
// this repo has a request-ID precedent to build on, and this repo's error paths are
// few enough that inline generation at each one is simpler than introducing a new
// middleware layer for it.
func logAndCorrelate(context string, err error) string {
	id := uuid.New().String()
	log.Printf("gateway: %s: [correlation_id=%s] %v", context, id, err)
	return id
}

// writeGenericError logs err's full detail (via logAndCorrelate) and writes a
// generic, correlation-ID-bearing error response to the client — the combined
// "don't leak, but stay correlatable" response half of the part 3 fix, factored out
// since both POST /invoice's 502 path and GET /invoice/{id}'s 500 path need the
// exact same shape.
func writeGenericError(w http.ResponseWriter, status int, logContext string, err error, clientMsg string) {
	id := logAndCorrelate(logContext, err)
	writeJSON(w, status, errorResponse{Error: clientMsg, CorrelationID: id})
}

// healthResponse is the JSON body returned by GET /health (task brief part 4,
// I13/I27) — deliberately a superset of the brief's exact
// {"status":"ok","wallet_connected":bool,"db_connected":bool} example, adding
// Version (see this file's `version` var doc comment) so an operator hitting
// /health can also confirm which build is actually running.
type healthResponse struct {
	Status          string `json:"status"`
	WalletConnected bool   `json:"wallet_connected"`
	DBConnected     bool   `json:"db_connected"`
	Version         string `json:"version"`
}

// healthCheckTimeout bounds how long GET /health's handler will wait on the
// wallet-connectivity/DB-ping checks below before giving up — task brief part 4
// (I13/I27): a health/readiness probe must itself resolve quickly and
// deterministically (an orchestrator polling this route on a short interval should
// never have its probe request itself hang indefinitely on a wedged dependency).
// 5s is a PLACEHOLDER, UNCONFIRMED value, same caveat as this file's other
// tunables — a generous-but-bounded guess for a same-host/same-network wallet
// gRPC daemon and Postgres instance, not load-tested or project-owner-approved.
const healthCheckTimeout = 5 * time.Second

// getWalletConnectivityFunc matches walletGRPC.GetWalletConnectivity's exact
// signature (task brief part 4: "CONFIRMED available in the exact pinned
// go-tari-lib dependency this repo uses"). Tests inject a fake instead of the real
// function so they never touch a real wallet gRPC connection or global walletGRPC
// package state — same DI pattern as invoice.ResolveAddressFunc/admin.IdentifyFunc.
type getWalletConnectivityFunc func() (*tari_generated.CheckConnectivityResponse, error)

// newHandler builds the Phase 1a HTTP surface: POST /invoice (create), GET
// /invoice/{id} (fetch current state), and (task brief part 4) GET /health
// (liveness/readiness probe, no auth). Returns the concrete *http.ServeMux (rather
// than the http.Handler interface) so main() can register Phase 1b's admin routes
// onto the same mux afterwards via adminServer.RegisterRoutes — see that method's own
// doc comment for why routes are composed this way instead of each package owning its
// own top-level handler.
//
// pool/getWalletConnectivity/buildVersion back GET /health's dependency checks and
// version stamp specifically — pool may be nil in tests that don't exercise
// /health (its DB check then always reports unreachable rather than panicking),
// and getWalletConnectivity may likewise be nil (its check then always reports
// unreachable).
func newHandler(store *invoice.Store, defaultTTL time.Duration, pool *pgxpool.Pool, getWalletConnectivity getWalletConnectivityFunc, buildVersion string) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /invoice", func(w http.ResponseWriter, r *http.Request) {
		// Body-size cap (task brief part 2, security-boundaries persona I5):
		// this is an unauthenticated, all-interfaces listener, so an
		// unbounded request body is a memory-exhaustion vector. A body over
		// the limit makes r.Body's next Read return an *http.MaxBytesError,
		// which json.NewDecoder surfaces as a normal Decode error below —
		// handled identically to any other malformed-body case, i.e. a
		// clean 400, never a panic/500.
		r.Body = http.MaxBytesReader(w, r.Body, maxInvoiceBodyBytes)

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
		// Upper-bound check (task brief part 2, security-boundaries persona
		// I5): amount_utari is persisted as a Postgres BIGINT (signed
		// int64) — see internal/invoice.Store.Create's `int64(inv.AmountUTari)`
		// cast. A value above math.MaxInt64 would silently become negative
		// there (two's-complement wraparound) and be misread back via
		// uint64() wraparound on the way out. Rejecting it here, before it
		// ever reaches store.Create, is simpler and clearer than trying to
		// detect/repair a wrapped-around value after the fact.
		if req.AmountUTari > math.MaxInt64 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("amount_utari must not exceed %d", uint64(math.MaxInt64)))
			return
		}

		inv, err := store.Create(r.Context(), req.OrderRef, req.AmountUTari, defaultTTL)
		if err != nil {
			// Wallet address resolution failure (or any other Create failure) is
			// reported as a 502: the request was well-formed, but this gateway
			// could not complete it because an upstream dependency (the wallet)
			// failed. The real error (which may contain the wallet gRPC dial
			// address) is logged server-side only — task brief part 3, I4
			// security-boundaries persona — never returned verbatim to this
			// unauthenticated client.
			writeGenericError(w, http.StatusBadGateway, "create invoice", err, "failed to create invoice: an upstream dependency (the wallet) could not complete this request")
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
			// Any other GetByID failure is a genuine internal error (e.g. a
			// pgx/SQL-level failure) — same "log full detail server-side,
			// return a generic correlatable message" treatment as the
			// wallet failure above (task brief part 3, I4 security-
			// boundaries persona): a raw pgx/SQL error string must never
			// reach this unauthenticated client.
			writeGenericError(w, http.StatusInternalServerError, "fetch invoice", err, "failed to fetch invoice")
			return
		}

		writeJSON(w, http.StatusOK, toInvoiceResponse(inv))
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		// No auth required (task brief part 4, I13/I27): this is an
		// infra-level liveness/readiness probe (e.g. a Docker HEALTHCHECK
		// or an orchestrator's readiness probe), not an admin route — see
		// docker/gateway/Dockerfile's HEALTHCHECK, which hits this route via
		// cmd/healthcheck's tiny standalone binary (distroless has no shell/
		// curl to do so directly).
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		defer cancel()

		walletConnected := false
		if getWalletConnectivity != nil {
			if resp, err := getWalletConnectivity(); err != nil {
				log.Printf("gateway: health: wallet connectivity check: %v", err)
			} else {
				walletConnected = resp.GetStatus() == tari_generated.CheckConnectivityResponse_Online
			}
		}

		dbConnected := false
		if pool != nil {
			if err := pool.Ping(ctx); err != nil {
				log.Printf("gateway: health: db ping: %v", err)
			} else {
				dbConnected = true
			}
		}

		status := http.StatusOK
		overall := "ok"
		if !walletConnected || !dbConnected {
			// 503, not 200 (task brief part 4's explicit requirement): an
			// orchestrator's health/readiness probe must actually reflect
			// readiness, not just "the HTTP listener answered".
			status = http.StatusServiceUnavailable
			overall = "unavailable"
		}

		writeJSON(w, status, healthResponse{
			Status:          overall,
			WalletConnected: walletConnected,
			DBConnected:     dbConnected,
			Version:         buildVersion,
		})
	})

	return mux
}
