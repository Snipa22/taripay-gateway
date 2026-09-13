// Package eventwatcher implements Phase 1b's background loop: consuming the wallet's
// live transaction-event stream and updating invoice status accordingly, firing a
// webhook exactly once per invoice-status transition.
package eventwatcher

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

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

// handleEvent processes a single transaction event: correlates it to an invoice by
// payment ID, maps its status, and — only on an actual status transition — updates
// the invoice and fires a webhook.
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
	inv, err := w.invoiceStore.GetByPaymentID(ctx, paymentID)
	if err != nil {
		if errors.Is(err, invoice.ErrNotFound) {
			// Could be an event for an invoice created before this gateway
			// started, or unrelated wallet activity — not an error.
			log.Printf("eventwatcher: no invoice found for payment_id %q, skipping (tx_id=%s)", paymentID, txn.TxId)
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

	mappedStatus, webhookEvent := mapStatus(txn.Status)
	previousStatus := inv.Status

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
		// event's txn.Amount in isolation) is what's compared below — this is
		// what lets a second, top-up transfer for the same invoice eventually
		// push it from underpaid to confirmed.
		total, err := w.invoiceStore.AddReceivedAmount(ctx, inv.ID, txn.Amount)
		if err != nil {
			log.Printf("eventwatcher: add received amount for invoice %s: %v", inv.ID, err)
			return
		}
		amountReceived = total

		if total < inv.AmountUTari {
			newStatus = invoice.StatusUnderpaid
			webhookEvent = "payment.underpaid"
		} else {
			now := time.Now().UTC()
			confirmedAt = &now
		}
	}

	if err := w.invoiceStore.UpdateStatus(ctx, inv.ID, newStatus, confirmedAt); err != nil {
		log.Printf("eventwatcher: update status for invoice %s to %q: %v", inv.ID, newStatus, err)
		return
	}

	if previousStatus == newStatus {
		// No actual transition (e.g. a repeat MINED_CONFIRMED event for an
		// already-confirmed invoice) — UpdateStatus above is harmless/idempotent,
		// but do NOT refire a webhook for it. This guard applies uniformly to
		// StatusUnderpaid too (per the task brief): if the same transaction is
		// somehow re-processed while the invoice is already underpaid, no
		// duplicate payment.underpaid webhook fires either.
		return
	}

	w.fireWebhook(ctx, inv, newStatus, webhookEvent, txn, confirmedAt, amountReceived)
}

// fireWebhook builds the webhook payload for a status transition, records a pending
// delivery row, and attempts delivery. If callbackURL is unconfigured, it logs and
// skips delivery entirely (invoice status has already been updated regardless).
func (w *Watcher) fireWebhook(ctx context.Context, inv *invoice.Invoice, newStatus, webhookEvent string, txn *tari_generated.TransactionEvent, confirmedAt *time.Time, amountReceived uint64) {
	if w.callbackURL == "" {
		log.Printf("eventwatcher: webhook callback URL not configured, skipping webhook for invoice %s (event=%s)", inv.ID, webhookEvent)
		return
	}

	payload := webhook.Payload{
		InvoiceID:           inv.ID.String(),
		PaymentID:           inv.PaymentID,
		OrderRef:            inv.OrderRef,
		Status:              newStatus,
		AmountUTari:         inv.AmountUTari,
		AmountReceivedUTari: amountReceived,
		TxID:                txn.TxId,
		Event:               webhookEvent,
	}
	if confirmedAt != nil {
		payload.ConfirmedAt = confirmedAt.Format(time.RFC3339)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("eventwatcher: marshal webhook payload for invoice %s: %v", inv.ID, err)
		return
	}

	delivery, err := w.webhookStore.Create(ctx, inv.ID, w.callbackURL, body)
	if err != nil {
		log.Printf("eventwatcher: create webhook delivery row for invoice %s: %v", inv.ID, err)
		return
	}

	if err := webhook.Attempt(ctx, w.webhookStore, w.sender, delivery); err != nil {
		log.Printf("eventwatcher: webhook delivery attempt bookkeeping for invoice %s: %v", inv.ID, err)
	}
}
