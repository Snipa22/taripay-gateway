// Package admin implements Phase 1b's HTMX admin UI: a wallet-health + recent-
// activity dashboard, a filterable invoice list, and a webhook-delivery log with a
// manual retry action.
//
// Same net/http + html/template + HTMX-via-CDN-script conventions as
// go-tari-ootle-explorer's internal/server (see that package's server.go/
// templates/layout.html) and go-crypto-pool-web's real-POST-action/flash-message
// pattern (see that repo's templates/layout.html and its .flash-message CSS class),
// per AGENTS.md's admin-panel convention.
package admin

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"

	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

//go:embed templates/*.html
var templateFS embed.FS

// staticFS embeds this package's vendored static assets (task brief part 5,
// I11/I15: self-host HTMX instead of loading it from unpkg.com) — same
// //go:embed mechanism as templateFS above. See static/htmx.min.js's own
// provenance note (below, on the route that serves it) for exactly what version
// this is and where it came from.
//
//go:embed static/*.js
var staticFS embed.FS

// DashboardInvoiceLimit/DashboardDeliveryLimit are how many rows GET /admin's
// "recent" panels show — per the task brief's "recent invoices (last 20, any
// status)" / "recent webhook deliveries (last 20)" spec.
const (
	DashboardInvoiceLimit  = 20
	DashboardDeliveryLimit = 20
)

// IdentifyFunc/GetBalancesFunc match walletGRPC.Identify/walletGRPC.GetBalances'
// exact signatures. Tests inject fakes instead of the real functions so they never
// touch a real wallet gRPC connection or global walletGRPC package state — same DI
// pattern Phase 1a used for invoice.Store's ResolveAddressFunc, and Phase 1b's own
// eventwatcher.EventSourceFunc.
type IdentifyFunc func() (*tari_generated.GetIdentityResponse, error)
type GetBalancesFunc func() (*tari_generated.GetBalanceResponse, error)

// Server holds every dependency this package's HTTP handlers need.
type Server struct {
	invoiceStore *invoice.Store
	webhookStore *webhook.Store
	sender       webhook.SenderInterface
	identify     IdentifyFunc
	getBalances  GetBalancesFunc

	dashboardTmpl  *template.Template
	invoicesTmpl   *template.Template
	deliveriesTmpl *template.Template
}

// New parses the embedded templates and constructs a Server. Returns an error if the
// templates fail to parse (a build-time programming error, not a runtime/request
// error).
func New(invoiceStore *invoice.Store, webhookStore *webhook.Store, sender webhook.SenderInterface, identify IdentifyFunc, getBalances GetBalancesFunc) (*Server, error) {
	dashboardTmpl, err := template.New("layout.html").ParseFS(templateFS, "templates/layout.html", "templates/dashboard.html", "templates/deliveries_panel.html")
	if err != nil {
		return nil, fmt.Errorf("admin: parse dashboard templates: %w", err)
	}
	invoicesTmpl, err := template.New("layout.html").ParseFS(templateFS, "templates/layout.html", "templates/invoices_list.html")
	if err != nil {
		return nil, fmt.Errorf("admin: parse invoices templates: %w", err)
	}
	deliveriesTmpl, err := template.New("deliveries-only").ParseFS(templateFS, "templates/deliveries_panel.html")
	if err != nil {
		return nil, fmt.Errorf("admin: parse deliveries panel template: %w", err)
	}

	return &Server{
		invoiceStore:   invoiceStore,
		webhookStore:   webhookStore,
		sender:         sender,
		identify:       identify,
		getBalances:    getBalances,
		dashboardTmpl:  dashboardTmpl,
		invoicesTmpl:   invoicesTmpl,
		deliveriesTmpl: deliveriesTmpl,
	}, nil
}

// RegisterRoutes registers every admin route (GET /admin, GET /admin/invoices, POST
// /admin/webhooks/{id}/retry) onto mux, each wrapped in requireAuth(authToken, ...) —
// the S1/I19 admin-auth fix (task brief "fix C2 and add auth", part 2). Every admin
// route goes through this same wrapping, with no exceptions (including the retry
// POST route, which is the one most likely to get missed in a quick fix).
//
// Taking an existing *http.ServeMux (rather than this package owning/returning its
// own top-level http.Handler, like go-tari-ootle-explorer's Server.Handler does) lets
// cmd/gateway/main.go compose this package's routes with its existing POST /invoice
// and GET /invoice/{id} routes on one mux, without either package needing to know
// about the other's route set.
//
// Callers are responsible for the "don't register admin routes at all if authToken is
// unconfigured" decision (per this repo's internal/config.Config.AdminAuthToken doc
// comment) — RegisterRoutes itself has no opinion on whether it should have been
// called; cmd/gateway/main.go is where that check lives, since that's where the
// resolved config (and thus the loud startup warning) is available.
func (s *Server) RegisterRoutes(mux *http.ServeMux, authToken string) {
	mux.Handle("GET /admin", requireAuth(authToken, http.HandlerFunc(s.handleDashboard)))
	mux.Handle("GET /admin/invoices", requireAuth(authToken, http.HandlerFunc(s.handleInvoices)))
	mux.Handle("POST /admin/webhooks/{id}/retry", requireAuth(authToken, http.HandlerFunc(s.handleWebhookRetry)))

	// GET /admin/static/htmx.min.js is deliberately NOT wrapped in
	// requireAuth (task brief part 5, I11/I15): it's a static asset (this
	// package's own vendored copy of htmx.org, not privileged merchant/order
	// data), so gating it behind the admin bearer token would only make the
	// dashboard itself harder to load for no confidentiality benefit — the
	// same reasoning go-tari-ootle-explorer and go-crypto-pool-web apply to
	// their own static assets. It's still only registered when RegisterRoutes
	// itself is called (i.e. only when AdminAuthToken is configured — see
	// cmd/gateway/main.go's registerAdminRoutes), consistent with "no admin
	// routes at all, including this one, when admin auth is unconfigured" —
	// though note this route serves no privileged data even when reachable,
	// so that consistency is a minor/cosmetic call, not a security necessity.
	mux.HandleFunc("GET /admin/static/htmx.min.js", s.handleHTMXStatic)
}

// handleHTMXStatic serves this package's vendored copy of htmx.org (task brief
// part 5, I11/I15): the admin UI used to load HTMX from unpkg.com with no
// Subresource Integrity hash and no fallback, so the one write action (webhook
// retry) silently did nothing if that CDN was ever unreachable. Vendoring it here
// removes that third-party runtime dependency entirely.
//
// static/htmx.min.js is htmx.org@1.9.12's dist/htmx.min.js, fetched directly from
// https://unpkg.com/htmx.org@1.9.12/dist/htmx.min.js (the exact version
// layout.html's <script> tag previously pointed at) at the time of this fix —
// verbatim, unmodified. Bump both this file and layout.html's cache-busting
// comment together if htmx is ever upgraded.
func (s *Server) handleHTMXStatic(w http.ResponseWriter, r *http.Request) {
	data, err := staticFS.ReadFile("static/htmx.min.js")
	if err != nil {
		// A missing embedded file here is a build-time packaging error, not
		// a request-time/user-facing one — still logged, but there's no
		// internal detail worth hiding from the client in this specific
		// case (this route serves no privileged data, per its own doc
		// comment above).
		log.Printf("admin: read embedded static/htmx.min.js: %v", err)
		http.Error(w, "static asset unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	if _, err := w.Write(data); err != nil {
		log.Printf("admin: write static/htmx.min.js response: %v", err)
	}
}

// ---- view adapters (presentation-only, keep invoice/webhook packages free of display
// concerns) ----

type invoiceView struct {
	*invoice.Invoice
}

func (v invoiceView) CreatedAtDisplay() string {
	return v.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC")
}

func (v invoiceView) ExpiresAtDisplay() string {
	return v.ExpiresAt.UTC().Format("2006-01-02 15:04:05 UTC")
}

func (v invoiceView) ConfirmedAtDisplay() string {
	if v.ConfirmedAt == nil {
		return "-"
	}
	return v.ConfirmedAt.UTC().Format("2006-01-02 15:04:05 UTC")
}

func toInvoiceViews(invoices []*invoice.Invoice) []invoiceView {
	out := make([]invoiceView, len(invoices))
	for i, inv := range invoices {
		out[i] = invoiceView{inv}
	}
	return out
}

type deliveryView struct {
	*webhook.Delivery
}

func (d deliveryView) CreatedAtDisplay() string {
	return d.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC")
}

func (d deliveryView) ResponseCodeDisplay() string {
	if d.ResponseCode == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *d.ResponseCode)
}

func (d deliveryView) LastErrorDisplay() string {
	if d.LastError == nil || *d.LastError == "" {
		return "-"
	}
	return *d.LastError
}

// IsFailed controls whether the retry button is shown for this row — only failed
// deliveries are retryable via the admin UI; pending/delivered rows have no action.
func (d deliveryView) IsFailed() bool {
	return d.Status == webhook.StatusFailed
}

func toDeliveryViews(deliveries []*webhook.Delivery) []deliveryView {
	out := make([]deliveryView, len(deliveries))
	for i, d := range deliveries {
		out[i] = deliveryView{d}
	}
	return out
}

// identityView/balanceView adapt the raw wallet gRPC response types for template
// display (hex-encoding the raw public-key/node-id byte slices).
type identityView struct {
	PublicAddress string
}

type balanceView struct {
	AvailableBalance       uint64
	PendingIncomingBalance uint64
	PendingOutgoingBalance uint64
	TimelockedBalance      uint64
}

// ---- handlers ----

// dashboardData is the template context for GET /admin. Deliveries/Flash/
// FlashIsError match deliveriesPanelData's field names exactly (see that struct's own
// doc comment) so the shared "deliveries" template block renders identically whether
// invoked as part of the full dashboard or standalone from the retry route.
type dashboardData struct {
	WalletError  string
	Identity     identityView
	Balance      balanceView
	Invoices     []invoiceView
	Deliveries   []deliveryView
	Flash        string
	FlashIsError bool
}

// logAndCorrelate logs the full detail of a server-side failure (context describes
// where it happened, err is the real error — e.g. a wallet gRPC dial address)
// alongside a freshly generated correlation ID, and returns that ID. Callers embed
// the returned ID in a generic, detail-free message rendered on the admin page —
// task brief part 3 (I4 security-boundaries persona): internal error text (a
// wallet gRPC dial address, in this package's case) must not be rendered in HTML
// verbatim, but an operator debugging a report still needs a way to find the
// matching full-detail log line. Same pattern, independently duplicated rather
// than shared, as cmd/gateway/main.go's identical helper of the same name — this
// package and that one don't currently share a common internal dependency this
// would be worth factoring into, and both are small enough that the duplication
// costs less than introducing one.
func logAndCorrelate(context string, err error) string {
	id := uuid.New().String()
	log.Printf("admin: %s: [correlation_id=%s] %v", context, id, err)
	return id
}

// handleDashboard serves GET /admin: wallet identity + balance, recent invoices (last
// DashboardInvoiceLimit, any status), recent webhook deliveries (last
// DashboardDeliveryLimit) with a retry button per failed row.
//
// Each panel degrades independently on its own query failure (matching
// go-tari-ootle-explorer's handleHome precedent) rather than failing the whole page.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	data := dashboardData{}

	identity, err := s.identify()
	if err != nil {
		// Generic message + correlation id (task brief part 3, I4 security-
		// boundaries persona): the real error here can contain the wallet
		// gRPC dial address, which must not be rendered on this admin page
		// verbatim — the full detail is still logged server-side via
		// logAndCorrelate above.
		cid := logAndCorrelate("dashboard: identify", err)
		data.WalletError = fmt.Sprintf("unable to reach wallet (identify failed) — see server logs (correlation_id: %s)", cid)
	} else {
		balance, err := s.getBalances()
		if err != nil {
			cid := logAndCorrelate("dashboard: get balances", err)
			data.WalletError = fmt.Sprintf("unable to reach wallet (get balances failed) — see server logs (correlation_id: %s)", cid)
		} else {
			data.Identity = identityView{PublicAddress: identity.GetPublicAddress()}
			data.Balance = balanceView{
				AvailableBalance:       balance.GetAvailableBalance(),
				PendingIncomingBalance: balance.GetPendingIncomingBalance(),
				PendingOutgoingBalance: balance.GetPendingOutgoingBalance(),
				TimelockedBalance:      balance.GetTimelockedBalance(),
			}
		}
	}

	invoices, err := s.invoiceStore.List(r.Context(), "", DashboardInvoiceLimit)
	if err != nil {
		log.Printf("admin: dashboard: list invoices: %v", err)
	} else {
		data.Invoices = toInvoiceViews(invoices)
	}

	deliveries, err := s.webhookStore.ListRecent(r.Context(), DashboardDeliveryLimit)
	if err != nil {
		log.Printf("admin: dashboard: list deliveries: %v", err)
	} else {
		data.Deliveries = toDeliveryViews(deliveries)
	}

	if err := s.dashboardTmpl.Execute(w, data); err != nil {
		log.Printf("admin: render dashboard: %v", err)
	}
}

// invoicesData is the template context for GET /admin/invoices.
type invoicesData struct {
	Invoices     []invoiceView
	StatusFilter string
}

// handleInvoices serves GET /admin/invoices: the full invoice list, optionally
// filtered by ?status=. An unrecognized status value simply matches zero rows
// (invoice.Store.List passes it straight through to the WHERE clause) rather than
// being rejected with a 400 — a stray/misspelled query param degrading to an empty
// (but still 200) list is friendlier for this internal ops tool.
func (s *Server) handleInvoices(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")

	invoices, err := s.invoiceStore.List(r.Context(), status, 0)
	if err != nil {
		http.Error(w, "failed to load invoices", http.StatusInternalServerError)
		log.Printf("admin: list invoices (status=%q): %v", status, err)
		return
	}

	data := invoicesData{Invoices: toInvoiceViews(invoices), StatusFilter: status}
	if err := s.invoicesTmpl.Execute(w, data); err != nil {
		log.Printf("admin: render invoices list: %v", err)
	}
}

// deliveriesPanelData is the template context for the standalone "deliveries" block,
// both as this route's direct HTMX-swap response and as embedded ({{template
// "deliveries" .}}) content inside dashboardData above.
type deliveriesPanelData struct {
	Deliveries   []deliveryView
	Flash        string
	FlashIsError bool
}

// handleWebhookRetry serves POST /admin/webhooks/{id}/retry: re-sends a specific
// webhook delivery (fetch it, re-Send via webhook.Attempt, which updates its record),
// then re-renders the deliveries panel (with a flash message describing the retry's
// outcome) as an HTMX-swappable fragment — this is the mechanism the admin UI's retry
// button calls (hx-post + hx-target="#webhook-deliveries" + hx-swap="outerHTML").
func (s *Server) handleWebhookRetry(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid delivery id", http.StatusBadRequest)
		return
	}

	delivery, err := s.webhookStore.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, webhook.ErrNotFound) {
			http.Error(w, "webhook delivery not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load webhook delivery", http.StatusInternalServerError)
		log.Printf("admin: retry: load delivery %s: %v", id, err)
		return
	}

	flash, flashIsError := s.retryAndDescribe(r.Context(), delivery)

	deliveries, err := s.webhookStore.ListRecent(r.Context(), DashboardDeliveryLimit)
	if err != nil {
		http.Error(w, "failed to reload webhook deliveries", http.StatusInternalServerError)
		log.Printf("admin: retry: reload deliveries: %v", err)
		return
	}

	data := deliveriesPanelData{Deliveries: toDeliveryViews(deliveries), Flash: flash, FlashIsError: flashIsError}
	if err := s.deliveriesTmpl.ExecuteTemplate(w, "deliveries", data); err != nil {
		log.Printf("admin: render deliveries panel: %v", err)
	}
}

// retryAndDescribe performs one retry attempt via webhook.Attempt and returns a
// human-readable flash message describing the outcome (and whether it should render
// as an error banner).
func (s *Server) retryAndDescribe(ctx context.Context, delivery *webhook.Delivery) (flash string, flashIsError bool) {
	if err := webhook.Attempt(ctx, s.webhookStore, s.sender, delivery); err != nil {
		log.Printf("admin: retry: attempt bookkeeping for delivery %s: %v", delivery.ID, err)
		return fmt.Sprintf("Retry attempted for delivery %s, but recording the result failed: %v", delivery.ID, err), true
	}

	updated, err := s.webhookStore.GetByID(ctx, delivery.ID)
	if err != nil {
		log.Printf("admin: retry: reload delivery %s after attempt: %v", delivery.ID, err)
		return fmt.Sprintf("Retry attempted for delivery %s, but its updated status could not be loaded.", delivery.ID), true
	}

	if updated.Status == webhook.StatusDelivered {
		code := 0
		if updated.ResponseCode != nil {
			code = *updated.ResponseCode
		}
		return fmt.Sprintf("Webhook delivery %s retried successfully (HTTP %d).", delivery.ID, code), false
	}

	errMsg := "unknown error"
	if updated.LastError != nil && *updated.LastError != "" {
		errMsg = *updated.LastError
	}
	return fmt.Sprintf("Webhook delivery %s retry failed: %s", delivery.ID, errMsg), true
}
