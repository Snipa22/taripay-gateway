// Package db provides this repo's Postgres access layer: a minimal, hand-rolled
// embedded-SQL migration runner.
//
// This mirrors sibling repo go-tari-ootle-explorer's internal/db/db.go pattern
// (embedded *.sql files via //go:embed, a schema_migrations tracking table, a
// DB.Migrate method) rather than pulling in a full migration framework like
// golang-migrate — AGENTS.md directs matching that repo's exact pattern rather than
// inventing a different migration approach, and that repo's own doc comment explains
// why it chose a hand-rolled runner over golang-migrate for a schema this small.
//
// Schema is intentionally minimal-viable for Phase 1a (see migrations/0001_init.up.sql)
// — Phase 1b (event-watcher/webhook state, admin UI) is expected to extend it
// incrementally, not have its needs guessed at here.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// DB wraps a pgx connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// Connect opens a pgx pool against dsn and returns a ready-to-use DB. Callers are
// responsible for calling Close when done. dsn is a standard Postgres connection
// string, e.g. "postgres://user:pass@localhost:5432/taripay?sslmode=disable".
func Connect(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return &DB{Pool: pool}, nil
}

// Close releases the underlying connection pool.
func (d *DB) Close() {
	d.Pool.Close()
}

// Migrate applies every embedded *.up.sql migration that hasn't already been recorded
// in schema_migrations, in filename order, each inside its own transaction.
func (d *DB) Migrate(ctx context.Context) error {
	if _, err := d.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("db: migrate: create schema_migrations: %w", err)
	}

	versions, err := upVersions()
	if err != nil {
		return err
	}

	for _, version := range versions {
		var alreadyApplied bool
		err := d.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&alreadyApplied)
		if err != nil {
			return fmt.Errorf("db: migrate: check %s: %w", version, err)
		}
		if alreadyApplied {
			continue
		}

		sqlBytes, err := migrationFS.ReadFile("migrations/" + version + ".up.sql")
		if err != nil {
			return fmt.Errorf("db: migrate: read %s: %w", version, err)
		}

		tx, err := d.Pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("db: migrate: begin %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: migrate: apply %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: migrate: record %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("db: migrate: commit %s: %w", version, err)
		}
	}
	return nil
}

// MigrateDown reverts every migration currently recorded in schema_migrations, in
// reverse filename order, by running each migration's matching *.down.sql file. Mainly
// intended for tests exercising the up-then-down round trip (see migrations_test.go) —
// there is no expectation cmd/gateway ever calls this in production.
func (d *DB) MigrateDown(ctx context.Context) error {
	versions, err := upVersions()
	if err != nil {
		return err
	}
	// Revert in reverse order of application.
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]

		var alreadyApplied bool
		err := d.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&alreadyApplied)
		if err != nil {
			return fmt.Errorf("db: migrate down: check %s: %w", version, err)
		}
		if !alreadyApplied {
			continue
		}

		sqlBytes, err := migrationFS.ReadFile("migrations/" + version + ".down.sql")
		if err != nil {
			return fmt.Errorf("db: migrate down: read %s: %w", version, err)
		}

		tx, err := d.Pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("db: migrate down: begin %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: migrate down: apply %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: migrate down: unrecord %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("db: migrate down: commit %s: %w", version, err)
		}
	}
	return nil
}

// upVersions returns every migration version (filename minus the ".up.sql" suffix)
// found under the embedded migrations/ dir, sorted ascending (i.e. application order).
func upVersions() ([]string, error) {
	entries, err := fs.Glob(migrationFS, "migrations/*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("db: glob migrations: %w", err)
	}
	versions := make([]string, 0, len(entries))
	for _, entry := range entries {
		versions = append(versions, strings.TrimSuffix(strings.TrimPrefix(entry, "migrations/"), ".up.sql"))
	}
	sort.Strings(versions)
	return versions, nil
}
