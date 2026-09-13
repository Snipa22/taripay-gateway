// Package invoice implements Phase 1a's invoice creation, lookup, status update, and
// expiry-sweep logic against Postgres, plus the wallet-address resolution needed to
// create a new invoice.
//
// Phase 1b (a separate, later dispatch — the event-watcher/webhook delivery loop and
// the HTMX admin UI) will import this package's Store without full context of how it
// was built, so its public surface is kept intentionally narrow: Create, GetByID,
// GetByPaymentID, UpdateStatus, ExpireStale. Don't grow this surface speculatively
// ahead of what Phase 1b actually needs.
package invoice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned by GetByID/GetByPaymentID when no matching invoice exists.
var ErrNotFound = errors.New("invoice: not found")

// Invoice mirrors the `invoices` table (see internal/db/migrations/0001_init.up.sql).
type Invoice struct {
	ID          uuid.UUID
	PaymentID   string
	OrderRef    string
	AmountUTari uint64
	Address     string
	Status      string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	ConfirmedAt *time.Time
}

// Status values for the invoices.status column.
const (
	StatusPending   = "pending"
	StatusSeen      = "seen"
	StatusConfirmed = "confirmed"
	StatusExpired   = "expired"
	StatusCancelled = "cancelled"
)

// ResolveAddressFunc resolves a payment ID to a wallet payment address. In production
// this is wired to walletGRPC.GetPaymentIdAddress (see cmd/gateway/main.go); tests
// inject a fake so they never touch a real wallet gRPC connection or global
// walletGRPC package state.
type ResolveAddressFunc func(paymentID string) (string, error)

// Store wraps a *pgxpool.Pool with this package's invoice persistence operations.
type Store struct {
	db             *pgxpool.Pool
	resolveAddress ResolveAddressFunc
}

// NewStore builds a Store backed by db, resolving wallet addresses via resolveAddress.
func NewStore(db *pgxpool.Pool, resolveAddress ResolveAddressFunc) *Store {
	return &Store{db: db, resolveAddress: resolveAddress}
}

// Create generates a new invoice: a UUID (used as both the row's id and, as its
// string form, the wallet payment_id — see the doc comment on that field below),
// resolves a payment address for it via the injected resolveAddress func, and inserts
// the row. If address resolution fails, no row is inserted (no orphan invoices with no
// valid address) and the error is returned as-is (wrapped) to the caller.
//
// payment_id is deliberately just id.String() rather than a separately-generated
// value: the brief's own suggestion, and it keeps the two trivially correlatable
// (looking up an invoice by either its DB id or its wallet payment_id always finds the
// same row) with no risk of them ever drifting apart.
func (s *Store) Create(ctx context.Context, orderRef string, amountUTari uint64, ttl time.Duration) (*Invoice, error) {
	id := uuid.New()
	paymentID := id.String()

	address, err := s.resolveAddress(paymentID)
	if err != nil {
		return nil, fmt.Errorf("invoice: resolve address for payment_id %s: %w", paymentID, err)
	}

	now := time.Now().UTC()
	inv := &Invoice{
		ID:          id,
		PaymentID:   paymentID,
		OrderRef:    orderRef,
		AmountUTari: amountUTari,
		Address:     address,
		Status:      StatusPending,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO invoices (id, payment_id, order_ref, amount_utari, address, status, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, inv.ID, inv.PaymentID, inv.OrderRef, int64(inv.AmountUTari), inv.Address, inv.Status, inv.CreatedAt, inv.ExpiresAt)
	if err != nil {
		return nil, fmt.Errorf("invoice: insert: %w", err)
	}

	return inv, nil
}

// GetByID looks up an invoice by its primary key. Returns ErrNotFound if no such
// invoice exists.
func (s *Store) GetByID(ctx context.Context, id uuid.UUID) (*Invoice, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, payment_id, order_ref, amount_utari, address, status, created_at, expires_at, confirmed_at
		FROM invoices WHERE id = $1
	`, id)
	return scanInvoice(row)
}

// GetByPaymentID looks up an invoice by its wallet payment_id. Returns ErrNotFound if
// no such invoice exists.
func (s *Store) GetByPaymentID(ctx context.Context, paymentID string) (*Invoice, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, payment_id, order_ref, amount_utari, address, status, created_at, expires_at, confirmed_at
		FROM invoices WHERE payment_id = $1
	`, paymentID)
	return scanInvoice(row)
}

// UpdateStatus sets an invoice's status (and, if non-nil, its confirmed_at) by id.
// Returns ErrNotFound if no such invoice exists.
func (s *Store) UpdateStatus(ctx context.Context, id uuid.UUID, status string, confirmedAt *time.Time) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE invoices SET status = $2, confirmed_at = $3 WHERE id = $1
	`, id, status, confirmedAt)
	if err != nil {
		return fmt.Errorf("invoice: update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ExpireStale sets any pending/seen invoice whose expires_at has passed to expired,
// and returns the count of rows affected.
func (s *Store) ExpireStale(ctx context.Context) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE invoices
		SET status = $1
		WHERE status IN ($2, $3) AND expires_at < now()
	`, StatusExpired, StatusPending, StatusSeen)
	if err != nil {
		return 0, fmt.Errorf("invoice: expire stale: %w", err)
	}
	return tag.RowsAffected(), nil
}

// rowScanner is the subset of pgx.Row's interface scanInvoice needs — satisfied by
// both pgxpool.Pool.QueryRow's return value, letting this helper serve both GetByID
// and GetByPaymentID above without duplicating the scan logic.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanInvoice(row rowScanner) (*Invoice, error) {
	var inv Invoice
	var amountUTari int64
	err := row.Scan(&inv.ID, &inv.PaymentID, &inv.OrderRef, &amountUTari, &inv.Address, &inv.Status, &inv.CreatedAt, &inv.ExpiresAt, &inv.ConfirmedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("invoice: scan: %w", err)
	}
	inv.AmountUTari = uint64(amountUTari)
	return &inv, nil
}
