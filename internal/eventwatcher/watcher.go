// Package eventwatcher implements Phase 1b's background loop: consuming the wallet's
// live transaction-event stream and updating invoice status accordingly, firing a
// webhook exactly once per invoice-status transition.
package eventwatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"

	"github.com/Snipa22/taripay-gateway/internal/invoice"
	"github.com/Snipa22/taripay-gateway/internal/webhook"
)

// EventSourceFunc matches walletGRPC.StreamTransactionEvents' exact signature. Tests
// inject a fake event source instead of the real function so they never touch a real
// wallet gRPC connection — same DI pattern Phase 1a used for invoice.Store's
// ResolveAddressFunc.
type EventSourceFunc func(ctx context.Context) (<-chan *tari_generated.TransactionEventResponse, <-chan error)

// transactionEventDirectionInbound is the wire value tari_generated.TransactionEvent.Direction
// (populated by StreamTransactionEvents) uses for an inbound payment: the short
// form "Inbound".
//
// This is DELIBERATELY NOT the same as the long enum-name form
// ("TRANSACTION_DIRECTION_INBOUND") that tari_generated.TransactionInfo.Direction
// (a different generated struct, populated by GetCompletedTransactions /
// GetBlockHeightTransactions) uses. Both fields represent "is this an inbound
// payment?", and it looks like they should share a wire format, but they don't:
//
//   - TransactionEvent.Direction/Status are plain proto `string` fields, and
//     minotari_console_wallet's gRPC server (applications/minotari_console_wallet/
//     src/grpc/mod.rs's convert_to_transaction_event) populates them by calling
//     `.to_string()` on the wallet's internal Rust TransactionDirection/
//     LegacyTransactionStatus enums, whose Display impls emit short, human-readable
//     forms ("Inbound", "Outbound"; "Mined Confirmed", "One-Sided Confirmed", etc.).
//   - TransactionInfo.Direction/Status ARE real proto enums (TransactionDirection/
//     TransactionStatus from wallet.proto), which protobuf/grpc serialize as their
//     full enum-name form ("TRANSACTION_DIRECTION_INBOUND",
//     "TRANSACTION_STATUS_MINED_CONFIRMED").
//
// Confirmed live on 2026-09-13 against a real minotari_console_wallet (Esmeralda
// testnet): a genuine inbound one-sided payment made StreamTransactionEvents emit
// direction: "Inbound", while GetCompletedTransactions for the SAME transaction
// returned direction: "TRANSACTION_DIRECTION_INBOUND" on TransactionInfo. Also cross-
// checked against minotari_console_wallet's own source
// (base_layer/common_types/src/transaction.rs's `impl Display for
// TransactionDirection`, which only ever emits "Inbound"/"Outbound"/"Unknown" — never
// the TRANSACTION_DIRECTION_* form). Do NOT add the long form here: this specific
// field/RPC never emits it, and doing so would just resurrect the confusion that
// caused this bug.
const transactionEventDirectionInbound = "Inbound"

// Watcher consumes a wallet transaction-event stream and drives invoice status
// updates + webhook delivery from it.
type Watcher struct {
	invoiceStore *invoice.Store
	webhookStore *webhook.Store
	sender       webhook.SenderInterface
	callbackURL  string
	events       EventSourceFunc
}

// NewWatcher builds a Watcher. callbackURL is the merchant's configured webhook
// callback URL (cfg.WebhookCallbackURL) — if empty, Run still processes events and
// updates invoice status, but skips webhook delivery entirely (logging once per
// skipped delivery), matching the "webhook delivery unconfigured is not fatal"
// treatment used for this same field in internal/config.
func NewWatcher(invoiceStore *invoice.Store, webhookStore *webhook.Store, sender webhook.SenderInterface, callbackURL string, events EventSourceFunc) *Watcher {
	return &Watcher{
		invoiceStore: invoiceStore,
		webhookStore: webhookStore,
		sender:       sender,
		callbackURL:  callbackURL,
		events:       events,
	}
}

// mapStatus maps a tari_generated.TransactionEvent's wallet-reported Status string to
// an invoice status + the webhook "event" field that should accompany that
// transition, per the task brief's exact mapping table.
//
// PLACEHOLDER, NEEDS REVIEW: this is a best-effort v1 heuristic derived from the real
// TransactionStatus enum names (tari_protos/wallet.proto), not a mapping the project
// owner (Alex) has explicitly signed off on — same "flag as unconfirmed" treatment as
// Phase 1a's DefaultConfirmationDepth/DefaultInvoiceTTLMinutes placeholders.
//
// This deliberately does NOT independently count confirmations against
// config.ConfirmationDepth: MINED_CONFIRMED is trusted as-is, on the assumption the
// wallet's own daemon-side confirmation threshold already decided when to emit that
// status. Doing our own block-height-based confirmation counting would require also
// consuming base-node chain height data this gateway doesn't have wired up in this
// pass — see the task brief's explicit "don't build that in this pass" instruction.
// config.ConfirmationDepth therefore remains unused/aspirational until that's built.
//
// IMPORTANT, LIVE-CONFIRMED 2026-09-13: unlike TransactionInfo.Status (from
// GetCompletedTransactions/GetBlockHeightTransactions, which really does use the
// long TRANSACTION_STATUS_* enum-name form, e.g. "TRANSACTION_STATUS_MINED_CONFIRMED"
// — confirmed via grpcurl against a real minotari_console_wallet on Esmeralda
// testnet), TransactionEvent.Status (this function's input, from
// StreamTransactionEvents) is a plain proto `string` field, not an enum, and the
// wallet daemon populates it with minotari_console_wallet's
// LegacyTransactionStatus/TransactionStatus Display impl's human-readable, spaced
// form instead: "Broadcast", "Pending", "Mined Unconfirmed", "Mined Confirmed",
// "One-Sided Unconfirmed", "One-Sided Confirmed", "Rejected", etc. — confirmed live
// by triggering real one-sided/self transactions against a real wallet and observing
// StreamTransactionEvents emit e.g. status="One-Sided Unconfirmed" then
// status="Mined Confirmed"/"One-Sided Confirmed" for the same transaction, and cross-
// checked directly against minotari_console_wallet's own source
// (base_layer/common_types/src/transaction.rs's `impl Display for
// LegacyTransactionStatus`). The matching below is therefore done against the
// space/hyphen-separated short form (case-insensitively, via an upper-cased
// comparison, to tolerate any casing drift) — NOT the TRANSACTION_STATUS_* long form,
// which this field never emits. Do not "fix" this back to underscore-separated
// TRANSACTION_STATUS_* matching; that is TransactionInfo.Status's format, not this
// one's.
func mapStatus(status string) (invoiceStatus, webhookEvent string) {
	upper := strings.ToUpper(status)
	switch {
	case strings.Contains(upper, "MINED CONFIRMED"), strings.Contains(upper, "ONE-SIDED CONFIRMED"):
		return invoice.StatusConfirmed, "payment.confirmed"
	case strings.Contains(upper, "BROADCAST"), strings.Contains(upper, "PENDING"),
		strings.Contains(upper, "MINED UNCONFIRMED"), strings.Contains(upper, "ONE-SIDED UNCONFIRMED"):
		return invoice.StatusSeen, "payment.seen"
	default:
		// REJECTED, NOT_FOUND, or any other/unrecognized status: treat as a
		// chain-level rejection, distinct from a merchant/customer-initiated
		// cancellation — see invoice.StatusRejected's doc comment for why this
		// is its own status value rather than reusing invoice.StatusCancelled.
		return invoice.StatusRejected, "payment.rejected"
	}
}

// isTerminalStatus reports whether status is one of invoice.TerminalStatuses — see
// that var's doc comment (task brief part 1, S2/AI-05 fix) for the full rationale.
// Used by handleEvent to guard against a later wallet event (a stray re-broadcast
// status echo, or a reorg-driven re-emit of an unconfirmed status for an
// already-mined tx) moving an already-terminal invoice backwards.
func isTerminalStatus(status string) bool {
	return invoice.TerminalStatuses[status]
}

// Run opens the wallet's transaction-event stream and processes events until the
// stream ends (cleanly, or with an error) or ctx is cancelled. On a stream error
// (non-nil value from the error channel) it logs and returns the error; on ctx
// cancellation it returns ctx.Err(); on a clean end of stream it returns nil.
//
// This function deliberately has NO retry/backoff logic of its own — restart-with-
// backoff on error is cmd/gateway/main.go's job (a supervisor loop around Run), per
// the task brief.
func (w *Watcher) Run(ctx context.Context) error {
	eventsCh, errCh := w.events(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err, ok := <-errCh:
			// A closed-with-no-value read on errCh isn't authoritative on its
			// own (see the eventsCh case below, which double-checks errCh
			// once the stream has actually ended) — only an actual non-nil
			// error is treated as terminal here. This avoids a race where
			// errCh closing (per go-tari-lib's StreamTransactionEvents, which
			// closes errCh before eventsCh) could be picked by select before
			// eventsCh's own remaining buffered events are drained.
			if ok && err != nil {
				log.Printf("eventwatcher: stream error: %v", err)
				return err
			}

		case ev, ok := <-eventsCh:
			if !ok {
				// Stream ended. Do one last non-blocking check of errCh for a
				// trailing error that may have been sent just before the
				// stream's goroutine closed both channels.
				select {
				case err, ok2 := <-errCh:
					if ok2 && err != nil {
						log.Printf("eventwatcher: stream error: %v", err)
						return err
					}
				default:
				}
				return nil
			}
			w.handleEvent(ctx, ev)
		}
	}
}

// handleEvent processes a single live-stream transaction event: filters to inbound
// events, then normalizes it down to handleTransaction's plain
// (paymentID, txID, status, amount) shape and calls that shared core logic. This
// function's own body is now ONLY the TransactionEventResponse-specific adapter
// step — see handleTransaction's doc comment for why the actual status-mapping/
// amount-check/terminal-guard/webhook decision logic lives there instead, shared
// with Reconcile's historical-transaction path.
func (w *Watcher) handleEvent(ctx context.Context, ev *tari_generated.TransactionEventResponse) {
	if ev == nil || ev.Transaction == nil {
		log.Printf("eventwatcher: received event with nil Transaction, skipping")
		return
	}
	txn := ev.Transaction

	// strings.EqualFold rather than == : minotari_console_wallet's gRPC server
	// hardcodes a lowercase "inbound"/"outbound" literal for one specific edge case
	// (a cancelled-while-still-pending transaction, handled via its
	// TransactionWrapper::Inbound/Outbound branches in grpc/mod.rs) instead of the
	// Display-derived "Inbound"/"Outbound" used everywhere else — EqualFold treats
	// both consistently without needing a second constant for that corner case. See
	// transactionEventDirectionInbound's doc comment for the full, live-confirmed
	// explanation of why this does NOT compare against
	// "TRANSACTION_DIRECTION_INBOUND".
	if !strings.EqualFold(txn.Direction, transactionEventDirectionInbound) {
		// Outbound events are this wallet's own sends, not incoming payments —
		// not an error, just not relevant to invoice tracking.
		log.Printf("eventwatcher: skipping non-inbound event (direction=%s, tx_id=%s)", txn.Direction, txn.TxId)
		return
	}

	paymentID := string(txn.UserPaymentId)
	w.handleTransaction(ctx, paymentID, txn.TxId, txn.Status, txn.Amount)
}

// handleTransaction is the shared core "given a payment_id-correlated status +
// amount, decide the new invoice state and possibly fire a webhook" logic —
// factored out of what used to be handleEvent's entire body (task brief part 1's
// prerequisite refactor) so there is exactly ONE implementation of this decision,
// called from two places: the live event-stream path (handleEvent above, adapting a
// *tari_generated.TransactionEvent) and the reconciliation path (Reconcile below,
// adapting a *tari_generated.TransactionInfo). Neither caller duplicates any of this
// logic — see Reconcile's doc comment for its own adapter step.
//
// status must already be in the same short, space/hyphen-separated wire form
// mapStatus expects (mapStatus itself is unchanged by this refactor — see that
// function's doc comment).
func (w *Watcher) handleTransaction(ctx context.Context, paymentID, txID, status string, amount uint64) {
	inv, err := w.invoiceStore.GetByPaymentID(ctx, paymentID)
	if err != nil {
		if errors.Is(err, invoice.ErrNotFound) {
			// Could be an event/transaction for an invoice created before this
			// gateway started, or unrelated wallet activity — not an error.
			log.Printf("eventwatcher: no invoice found for payment_id %q, skipping (tx_id=%s)", paymentID, txID)
			return
		}
		// A transient DB error on lookup: log and skip this event rather than
		// crashing the whole watcher loop over one bad read. This does mean a
		// transient DB blip can silently drop an event's effect — acceptable
		// for v1 (the wallet will typically emit further events for the same
		// payment as it progresses through statuses, giving another chance to
		// pick it up), but flagged here as a known v1 tradeoff.
		log.Printf("eventwatcher: lookup invoice for payment_id %q: %v (skipping event)", paymentID, err)
		return
	}

	mappedStatus, webhookEvent := mapStatus(status)
	previousStatus := inv.Status

	// Terminal-state guard (task brief part 1, S2/AI-05 fix): once an invoice has
	// reached a terminal status (invoice.TerminalStatuses — confirmed, rejected,
	// expired, cancelled), NO later event may change it, full stop, regardless of
	// what mapStatus computed for this event. This must run before the
	// AddReceivedAmount/UpdateStatus/webhook logic below entirely — not just
	// before UpdateStatus — so a stray re-broadcast/reorg-driven event on an
	// already-terminal invoice also can't corrupt AmountReceivedUTari's running
	// total. This is not an error: reorgs and duplicate/stray events are expected
	// wallet behavior, not a bug in this gateway.
	if isTerminalStatus(previousStatus) {
		log.Printf("eventwatcher: invoice %s already in terminal status %q, ignoring post-terminal event (tx_id=%s, computed_status=%q)", inv.ID, previousStatus, txID, mappedStatus)
		return
	}

	// newStatus/confirmedAt/amountReceived default to mappedStatus's own verdict
	// and are only overridden below for the confirmed-vs-underpaid decision (the
	// C2 fix, task brief part 1). This is deliberately scoped to ONLY the
	// mapStatus-computed-StatusConfirmed case: a chain-level status downgrade on
	// a later event (e.g. mapStatus returning StatusRejected) must not re-run
	// amount accumulation, per the brief's explicit "write this defensively"
	// instruction.
	newStatus := mappedStatus
	var confirmedAt *time.Time
	amountReceived := inv.AmountReceivedUTari

	if mappedStatus == invoice.StatusConfirmed {
		// Design decision (per the task brief, not reopened here): "received >=
		// invoiced" is the pass condition. Exact-match would be too strict for
		// real-world transfer-fee/rounding edge cases; no check at all (the
		// original C2 bug) let anyone who knew the payment_id confirm an
		// invoice for an arbitrary, possibly much smaller, amount. Overpayment
		// is accepted and confirms normally — a merchant can always manually
		// refund/credit the difference; underpayment does NOT confirm, and
		// instead lands the invoice in StatusUnderpaid (see that constant's
		// doc comment) until a later event's cumulative total catches up.
		//
		// AddReceivedAmount does an atomic accumulate-and-return rather than a
		// read-then-write, so the invoice's running total (not just this one
		// event's amount in isolation) is what's compared below — this is
		// what lets a second, top-up transfer for the same invoice eventually
		// push it from underpaid to confirmed.
		total, err := w.invoiceStore.AddReceivedAmount(ctx, inv.ID, amount)
		if err != nil {
			log.Printf("eventwatcher: add received amount for invoice %s: %v", inv.ID, err)
			return
		}
		amountReceived = total

		switch {
		case total < inv.AmountUTari:
			newStatus = invoice.StatusUnderpaid
			webhookEvent = "payment.underpaid"
		case time.Now().UTC().After(inv.ExpiresAt):
			// TTL confirm-time check (task brief part 2, item 2): a periodic
			// ExpireStale sweep (see cmd/gateway's runTTLSweepLoop) alone can
			// still lose a race against a payment landing in the exact window
			// between the invoice's expires_at passing and the next sweep
			// tick — this check closes that window at the only point that
			// actually matters, confirm time, rather than relying on sweep
			// timing alone. A payment that would otherwise confirm but
			// arrives after expires_at is a genuinely interesting case (a
			// late payment to an abandoned order) worth its own loud log
			// line, not a silent drop — the merchant can still see it via
			// AmountReceivedUTari (already updated by AddReceivedAmount
			// above) and the invoice's `expired` status through GetByID/
			// List/the admin UI even though no webhook fires for it.
			//
			// Decision (call this out explicitly, not an oversight, per the
			// task brief): this does NOT introduce a new `payment.late`
			// webhook event in this pass. Doing so would be a merchant-
			// plugin-contract change (a new event value the WooCommerce
			// plugin, built separately, would need to learn to handle), not
			// just a gateway-internal fix — out of scope here and flagged
			// for the project owner (Alex) to decide on separately, same
			// "flag as unconfirmed, don't silently decide it" treatment
			// AGENTS.md requires for the confirmation-depth/TTL/overpayment-
			// tolerance defaults.
			log.Printf("eventwatcher: LATE PAYMENT: invoice %s (payment_id=%s, order_ref=%s) received a confirming amount (%d/%d utari, tx_id=%s) AFTER its expires_at (%s, now %s) — leaving status expired, NOT firing payment.confirmed", inv.ID, inv.PaymentID, inv.OrderRef, total, inv.AmountUTari, txID, inv.ExpiresAt.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
			if err := w.invoiceStore.UpdateStatus(ctx, inv.ID, invoice.StatusExpired, nil); err != nil {
				log.Printf("eventwatcher: update status for invoice %s to %q (late payment): %v", inv.ID, invoice.StatusExpired, err)
			}
			return
		default:
			now := time.Now().UTC()
			confirmedAt = &now
		}
	}

	if previousStatus == newStatus {
		// No actual transition (e.g. a repeat MINED_CONFIRMED event for an
		// already-confirmed invoice) — UpdateStatus is harmless/idempotent, but
		// do NOT refire a webhook for it, and no delivery row is needed, so no
		// atomicity concern here. This guard applies uniformly to StatusUnderpaid
		// too (per the task brief): if the same transaction is somehow
		// re-processed while the invoice is already underpaid, no duplicate
		// payment.underpaid webhook fires either.
		if err := w.invoiceStore.UpdateStatus(ctx, inv.ID, newStatus, confirmedAt); err != nil {
			log.Printf("eventwatcher: update status for invoice %s to %q: %v", inv.ID, newStatus, err)
		}
		return
	}

	w.fireWebhook(ctx, inv, newStatus, webhookEvent, txID, confirmedAt, amountReceived)
}

// fireWebhook handles an actual status transition (previousStatus != newStatus,
// already established by handleTransaction): if no webhook callback URL is
// configured, it just performs the plain invoice.Store.UpdateStatus (no delivery row
// is needed at all in that case — see NewWatcher's doc comment). Otherwise it
// atomically commits the status update together with a new pending delivery row
// (task brief part 2, item 1 — see updateStatusAndCreateDelivery), then attempts
// delivery. txID is the correlated transaction's ID as a string — TransactionEvent's
// TxId is already a string; TransactionInfo's TxId is a uint64 that Reconcile's
// adapter step formats to a string before reaching handleTransaction/fireWebhook, so
// this function itself needs no knowledge of which wallet-gRPC shape it came from.
func (w *Watcher) fireWebhook(ctx context.Context, inv *invoice.Invoice, newStatus, webhookEvent, txID string, confirmedAt *time.Time, amountReceived uint64) {
	if w.callbackURL == "" {
		log.Printf("eventwatcher: webhook callback URL not configured, skipping webhook for invoice %s (event=%s)", inv.ID, webhookEvent)
		if err := w.invoiceStore.UpdateStatus(ctx, inv.ID, newStatus, confirmedAt); err != nil {
			log.Printf("eventwatcher: update status for invoice %s to %q: %v", inv.ID, newStatus, err)
		}
		return
	}

	payload := webhook.Payload{
		InvoiceID:           inv.ID.String(),
		PaymentID:           inv.PaymentID,
		OrderRef:            inv.OrderRef,
		Status:              newStatus,
		AmountUTari:         inv.AmountUTari,
		AmountReceivedUTari: amountReceived,
		TxID:                txID,
		Event:               webhookEvent,
	}
	if confirmedAt != nil {
		payload.ConfirmedAt = confirmedAt.Format(time.RFC3339)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		// json.Marshal of this fixed, simple struct essentially never fails in
		// practice, but if it somehow does, fall back to a plain (non-atomic)
		// status update rather than losing the transition entirely — there is
		// no payload to create a delivery row with anyway, so atomicity with a
		// delivery row is moot here.
		log.Printf("eventwatcher: marshal webhook payload for invoice %s: %v (status update proceeding without a webhook)", inv.ID, err)
		if err := w.invoiceStore.UpdateStatus(ctx, inv.ID, newStatus, confirmedAt); err != nil {
			log.Printf("eventwatcher: update status for invoice %s to %q: %v", inv.ID, newStatus, err)
		}
		return
	}

	delivery, err := w.updateStatusAndCreateDelivery(ctx, inv.ID, newStatus, confirmedAt, body)
	if err != nil {
		log.Printf("eventwatcher: atomic status update + webhook delivery row creation for invoice %s: %v", inv.ID, err)
		return
	}

	if err := webhook.Attempt(ctx, w.webhookStore, w.sender, delivery); err != nil {
		log.Printf("eventwatcher: webhook delivery attempt bookkeeping for invoice %s: %v", inv.ID, err)
	}
}

// updateStatusAndCreateDelivery is the webhook-delivery-reliability fix (task brief
// part 2, item 1): it commits an invoice's status update and its corresponding
// pending webhook-delivery row in a SINGLE database transaction, so it is impossible
// for one to succeed without the other. Before this fix, w.invoiceStore.UpdateStatus
// and w.webhookStore.Create were two independent statements — if Create failed after
// UpdateStatus had already committed (e.g. a crash, or a transient DB blip between
// the two calls), the invoice would have silently transitioned with NO delivery row
// ever created for it, and thus nothing for RetryFailedDeliveries (task brief part 2,
// item 2) to ever find and retry. Wrapping both writes in one pgx.Tx closes that
// window entirely: either both the UPDATE and the INSERT commit, or neither does.
//
// This deliberately begins the transaction on w.invoiceStore.Pool() (see that
// method's doc comment on why this only works because invoiceStore and webhookStore
// are always constructed against the same underlying *pgxpool.Pool in this repo) —
// there is no "transaction spanning two Stores" primitive in either package, and
// building one felt like overkill for what is, underneath, one pool being asked for
// one transaction that two different Store methods happen to write through.
//
// The delivery's actual HTTP send attempt (webhook.Attempt) deliberately happens
// AFTER this function returns/commits, not inside the same transaction: Attempt does
// its own separate, already-idempotent-enough bookkeeping writes (IncrementAttempts,
// then MarkDelivered/MarkFailed), and a real outbound HTTP call has no business
// holding a database transaction open around it.
func (w *Watcher) updateStatusAndCreateDelivery(ctx context.Context, invoiceID uuid.UUID, newStatus string, confirmedAt *time.Time, payload []byte) (*webhook.Delivery, error) {
	tx, err := w.invoiceStore.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	// Rollback is a safe no-op if Commit below already succeeded (pgx.Tx.Rollback's
	// own doc comment: safe to call multiple times / after a closed tx).
	defer func() { _ = tx.Rollback(ctx) }()

	if err := w.invoiceStore.UpdateStatusTx(ctx, tx, invoiceID, newStatus, confirmedAt); err != nil {
		return nil, fmt.Errorf("update status (tx): %w", err)
	}

	delivery, err := w.webhookStore.CreateTx(ctx, tx, invoiceID, w.callbackURL, payload)
	if err != nil {
		return nil, fmt.Errorf("create delivery row (tx): %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return delivery, nil
}

// GetCompletedByPaymentIDFunc matches walletGRPC.GetCompletedTransactionsByPaymentID's
// exact signature — confirmed directly against the real go-tari-lib@def118969dbc
// dependency this repo pins (see that function's own doc comment in
// go-tari-lib/walletGRPC/wallet.go). Tests inject a fake instead of the real
// function, same DI pattern as EventSourceFunc above, so they never touch a real
// wallet gRPC connection.
type GetCompletedByPaymentIDFunc func(paymentID string) ([]*tari_generated.TransactionInfo, error)

// Reconcile is the reconciliation-after-downtime fix (task brief part 1, S2/I7): the
// live event-stream (Run/handleEvent above) only ever sees transaction events that
// are emitted WHILE it is connected — any payment that lands during a deploy, a
// crash-restart, or the restart-with-backoff window in cmd/gateway's
// runEventWatcherWithBackoff is missed forever unless something else catches it up
// afterwards. Reconcile is that catch-up: for every invoice this gateway still
// considers non-terminal (invoice.Store.ListNonTerminal — pending, seen, underpaid),
// it calls getCompletedByPaymentID with that invoice's PaymentID, and for every
// returned TransactionInfo, applies the exact same status-mapping/amount-check/
// terminal-guard/webhook decision handleEvent's live-stream path uses — via the
// shared handleTransaction helper, so neither path duplicates that logic (see
// handleTransaction's doc comment).
//
// TransactionInfo's Direction/Status fields use the long enum-name wire form
// (unlike TransactionEvent's short string form — see
// transactionEventDirectionInbound's doc comment for the full, live-confirmed
// explanation of why these two structs differ), and TxId is a uint64 rather than a
// string, so this function is also where that adaptation happens: transactionInfoIsInbound
// checks Direction, transactionInfoStatusText normalizes Status down to the same
// short form mapStatus expects, and strconv.FormatUint converts TxId to a string —
// all so handleTransaction itself never needs to know which wallet-gRPC shape a
// given call originated from.
//
// Returns the number of non-terminal invoices actually examined (regardless of
// whether any of them turned out to have a matching completed transaction) — the
// task brief's (int, error) return shape doesn't pin down exactly what the int
// means, so this is the definition chosen and documented here. A non-nil error is
// only returned for an infra-level failure listing non-terminal invoices in the
// first place (ListNonTerminal); a per-invoice getCompletedByPaymentID failure is
// logged and that invoice is skipped (not fatal to the rest of the pass) — same
// "log and keep going" precedent handleTransaction itself uses for a transient
// invoice-lookup failure.
//
// KNOWN LIMITATION, flagged rather than silently worked around (per AGENTS.md's
// "don't silently pick... without citing it clearly" rule): getCompletedByPaymentID
// returns EVERY known transaction for a payment ID, not just ones not yet seen by
// this gateway — there is no tx_id-level dedup here or in AddReceivedAmount. For the
// common case this fix targets (an invoice that missed its ONE confirming
// transaction entirely while this gateway was down) that's exactly right: the first
// Reconcile pass after restart applies it once, the invoice becomes terminal
// (confirmed/underpaid-then-later-confirmed), and no further Reconcile pass ever
// re-examines it. But an invoice that stays non-terminal (StatusUnderpaid) across
// MULTIPLE restarts would have its already-counted historical transaction(s)
// re-applied on each subsequent Reconcile pass, double-counting AmountReceivedUTari.
// Closing that fully would need a tx_id-level idempotency table (a real schema
// addition), which is out of scope for this pass — flagged here for the project
// owner (Alex) rather than treated as solved.
func (w *Watcher) Reconcile(ctx context.Context, getCompletedByPaymentID GetCompletedByPaymentIDFunc) (int, error) {
	invoices, err := w.invoiceStore.ListNonTerminal(ctx)
	if err != nil {
		return 0, fmt.Errorf("eventwatcher: reconcile: list non-terminal invoices: %w", err)
	}

	examined := 0
	for _, inv := range invoices {
		if ctx.Err() != nil {
			return examined, ctx.Err()
		}
		examined++

		txns, err := getCompletedByPaymentID(inv.PaymentID)
		if err != nil {
			log.Printf("eventwatcher: reconcile: get completed transactions for payment_id %q (invoice %s): %v (skipping)", inv.PaymentID, inv.ID, err)
			continue
		}

		for _, txn := range txns {
			if txn == nil {
				continue
			}
			if !transactionInfoIsInbound(txn) {
				// Outbound (this wallet's own sends) — not relevant to invoice
				// tracking, same treatment as handleEvent's own direction
				// filter above.
				continue
			}
			txID := strconv.FormatUint(txn.TxId, 10)
			status := transactionInfoStatusText(txn.Status)
			w.handleTransaction(ctx, inv.PaymentID, txID, status, txn.Amount)
		}
	}
	return examined, nil
}

// transactionInfoIsInbound reports whether txn represents an inbound payment, per
// TransactionInfo.Direction's real proto enum (as opposed to
// TransactionEvent.Direction's plain string field — see
// transactionEventDirectionInbound's doc comment for the full format-difference
// rationale this mirrors).
func transactionInfoIsInbound(txn *tari_generated.TransactionInfo) bool {
	return txn.Direction == tari_generated.TransactionDirection_TRANSACTION_DIRECTION_INBOUND
}

// transactionInfoStatusText converts a TransactionInfo.Status enum value into the
// same short, space/hyphen-separated status text form mapStatus expects (mirroring
// TransactionEvent.Status's wire format) — see transactionEventDirectionInbound's
// doc comment for the full TransactionInfo-vs-TransactionEvent format-difference
// rationale this mirrors for Status too. TransactionInfo.Status is a real proto enum
// (unlike TransactionEvent.Status's plain string field), and its own .String() form
// is the long TRANSACTION_STATUS_* enum-name form (e.g.
// "TRANSACTION_STATUS_MINED_CONFIRMED"), which mapStatus's Contains-based matching
// does NOT understand as-is: the enum-name form uses underscores throughout (no
// hyphens at all), so a naive underscore-to-space substitution would turn
// "TRANSACTION_STATUS_ONE_SIDED_CONFIRMED" into "ONE SIDED CONFIRMED" — missing the
// hyphen mapStatus's "ONE-SIDED CONFIRMED" case actually requires. Rather than teach
// mapStatus two incompatible wire formats, this small, explicit adapter table
// converts every known TransactionStatus value to the exact short form its
// TransactionEvent.Status counterpart would use for the equivalent chain state, so
// mapStatus itself needs no changes to serve both callers.
func transactionInfoStatusText(status tari_generated.TransactionStatus) string {
	switch status {
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_COMPLETED:
		return "Completed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_BROADCAST:
		return "Broadcast"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_MINED_UNCONFIRMED:
		return "Mined Unconfirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_IMPORTED:
		return "Imported"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_PENDING:
		return "Pending"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_COINBASE:
		return "Coinbase"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_MINED_CONFIRMED:
		return "Mined Confirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_REJECTED:
		return "Rejected"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_ONE_SIDED_UNCONFIRMED:
		return "One-Sided Unconfirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_ONE_SIDED_CONFIRMED:
		return "One-Sided Confirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_QUEUED:
		return "Queued"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_NOT_FOUND:
		return "Not Found"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_COINBASE_UNCONFIRMED:
		return "Coinbase Unconfirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_COINBASE_CONFIRMED:
		return "Coinbase Confirmed"
	case tari_generated.TransactionStatus_TRANSACTION_STATUS_COINBASE_NOT_IN_BLOCK_CHAIN:
		return "Coinbase Not In Block Chain"
	default:
		// Genuinely unrecognized enum value (e.g. a future wallet.proto addition
		// this gateway doesn't know about yet) — fall back to the enum's own
		// String() form. mapStatus's default branch will treat this as
		// rejection-shaped, same as any other unrecognized status string.
		return status.String()
	}
}
