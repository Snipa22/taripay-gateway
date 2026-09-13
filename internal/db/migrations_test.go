package db

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// testDSN returns the DSN to use for live-Postgres tests in this package:
// TARIPAY_TEST_POSTGRES_DSN if set (this sandbox has a real Postgres 17 instance at
// postgresql://taripay@127.0.0.1:5433/taripay_test — see the task brief), otherwise
// empty. Tests SKIP rather than fail when this is unset, so this package's test suite
// still runs clean in an environment with no Postgres at all — but note that in THIS
// sandbox the env var is set and these tests do run for real, not skip.
func testDSN() string {
	return os.Getenv("TARIPAY_TEST_POSTGRES_DSN")
}

// TestMigrationsExist is a basic sanity check that the embedded migration files exist
// and are non-empty, independent of any live Postgres connection.
func TestMigrationsExist(t *testing.T) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}

	var sqlFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			sqlFiles = append(sqlFiles, e.Name())
		}
	}
	if len(sqlFiles) == 0 {
		t.Fatal("no *.sql migration files found under migrations/ - expected at least 0001_init.up.sql/.down.sql")
	}
	for _, name := range sqlFiles {
		raw, err := os.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.TrimSpace(string(raw)) == "" {
			t.Fatalf("%s is empty", name)
		}
	}
}

// TestMigrate_UpThenDownAgainstRealPostgres is the brief's required "runs migrations up
// then down against a throwaway Postgres" test. This sandbox has a real Postgres 17
// instance reachable via TARIPAY_TEST_POSTGRES_DSN, so this actually executes against
// it — see the package's testDSN() doc comment for the skip fallback in environments
// without one.
func TestMigrate_UpThenDownAgainstRealPostgres(t *testing.T) {
	dsn := testDSN()
	if dsn == "" {
		t.Skip("SKIP: TARIPAY_TEST_POSTGRES_DSN not set, no live Postgres to test against")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer database.Close()

	// Start from a clean slate so this test is repeatable against a persistent
	// dev/test Postgres instance, not just a throwaway one.
	if _, err := database.Pool.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations, webhook_deliveries, invoices CASCADE`); err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}

	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	var invoicesExists bool
	if err := database.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'invoices')`,
	).Scan(&invoicesExists); err != nil {
		t.Fatalf("check invoices table exists: %v", err)
	}
	if !invoicesExists {
		t.Fatal("expected table \"invoices\" to exist after Migrate(), it does not")
	}

	var recordedVersion string
	if err := database.Pool.QueryRow(ctx, `SELECT version FROM schema_migrations WHERE version = $1`, "0001_init").Scan(&recordedVersion); err != nil {
		t.Fatalf("expected schema_migrations to record 0001_init: %v", err)
	}

	// Running Migrate again must be a clean no-op (the "already applied" skip path),
	// not a duplicate-table error.
	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() call error = %v, want nil (idempotent no-op)", err)
	}

	// Now the down path: MigrateDown must drop invoices and unrecord the migration.
	if err := database.MigrateDown(ctx); err != nil {
		t.Fatalf("MigrateDown() error = %v", err)
	}

	if err := database.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'invoices')`,
	).Scan(&invoicesExists); err != nil {
		t.Fatalf("check invoices table exists after down: %v", err)
	}
	if invoicesExists {
		t.Fatal("expected table \"invoices\" to be dropped after MigrateDown(), it still exists")
	}

	var stillRecorded bool
	if err := database.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, "0001_init",
	).Scan(&stillRecorded); err != nil {
		t.Fatalf("check schema_migrations after down: %v", err)
	}
	if stillRecorded {
		t.Fatal("expected 0001_init to be unrecorded from schema_migrations after MigrateDown(), it still is")
	}

	// Running MigrateDown again must be a clean no-op too (nothing left to revert).
	if err := database.MigrateDown(ctx); err != nil {
		t.Fatalf("second MigrateDown() call error = %v, want nil (idempotent no-op)", err)
	}

	// Re-apply Up so the DB is left in a known-good state for other tests in this
	// package/module that assume the schema exists (e.g. internal/invoice's tests
	// against the same TARIPAY_TEST_POSTGRES_DSN database).
	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("re-apply Migrate() after down error = %v", err)
	}
}
