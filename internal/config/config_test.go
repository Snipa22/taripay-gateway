package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoad_DefaultsApplyWhenNothingSet covers the "no flag, no env, no config file"
// path for every field that has a real default. PostgresDSN has no default (see
// TestLoad_MissingPostgresDSNErrors below), so it must be supplied for this test to
// reach the other fields' defaults.
func TestLoad_DefaultsApplyWhenNothingSet(t *testing.T) {
	cfg, err := Load(Flags{PostgresDSN: "postgres://example/db"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.WalletGRPCAddress != DefaultWalletGRPCAddress {
		t.Errorf("WalletGRPCAddress = %q, want %q", cfg.WalletGRPCAddress, DefaultWalletGRPCAddress)
	}
	if cfg.HTTPListenAddr != DefaultHTTPListenAddr {
		t.Errorf("HTTPListenAddr = %q, want %q", cfg.HTTPListenAddr, DefaultHTTPListenAddr)
	}
	if cfg.ConfirmationDepth != DefaultConfirmationDepth {
		t.Errorf("ConfirmationDepth = %d, want %d", cfg.ConfirmationDepth, DefaultConfirmationDepth)
	}
	if cfg.InvoiceTTLMinutes != DefaultInvoiceTTLMinutes {
		t.Errorf("InvoiceTTLMinutes = %d, want %d", cfg.InvoiceTTLMinutes, DefaultInvoiceTTLMinutes)
	}
}

// TestLoad_EnvVarsOverrideDefaults covers every env var overriding its default.
func TestLoad_EnvVarsOverrideDefaults(t *testing.T) {
	t.Setenv(envWalletGRPCAddress, "10.0.0.5:18143")
	t.Setenv(envPostgresDSN, "postgres://env-user:env-pass@env-host:5432/env_db")
	t.Setenv(envHTTPListenAddr, ":9999")
	t.Setenv(envConfirmationDepth, "10")
	t.Setenv(envInvoiceTTLMinutes, "60")

	cfg, err := Load(Flags{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.WalletGRPCAddress != "10.0.0.5:18143" {
		t.Errorf("WalletGRPCAddress = %q, want %q", cfg.WalletGRPCAddress, "10.0.0.5:18143")
	}
	if cfg.PostgresDSN != "postgres://env-user:env-pass@env-host:5432/env_db" {
		t.Errorf("PostgresDSN = %q, want env value", cfg.PostgresDSN)
	}
	if cfg.HTTPListenAddr != ":9999" {
		t.Errorf("HTTPListenAddr = %q, want %q", cfg.HTTPListenAddr, ":9999")
	}
	if cfg.ConfirmationDepth != 10 {
		t.Errorf("ConfirmationDepth = %d, want 10", cfg.ConfirmationDepth)
	}
	if cfg.InvoiceTTLMinutes != 60 {
		t.Errorf("InvoiceTTLMinutes = %d, want 60", cfg.InvoiceTTLMinutes)
	}
}

// TestLoad_MissingPostgresDSNErrors covers the "must error clearly if unset" rule
// (AGENTS.md / brief section 2): no flag, no env, no config file for PostgresDSN must
// produce a clear, actionable error rather than silently defaulting.
func TestLoad_MissingPostgresDSNErrors(t *testing.T) {
	_, err := Load(Flags{})
	if err == nil {
		t.Fatal("Load() error = nil, want an error when PostgresDSN is unset everywhere")
	}
	if !strings.Contains(err.Error(), "postgres DSN is required") {
		t.Errorf("Load() error = %q, want it to mention the missing postgres DSN clearly", err.Error())
	}
}

// TestLoad_PrecedenceFlagOverEnvOverFileOverDefault verifies the full precedence chain
// for a single field (PostgresDSN), one override layer at a time.
func TestLoad_PrecedenceFlagOverEnvOverFileOverDefault(t *testing.T) {
	fileDSN := "postgres://file-user:file-pass@file-host:5432/file_db"
	envDSN := "postgres://env-user:env-pass@env-host:5432/env_db"
	flagDSN := "postgres://flag-user:flag-pass@flag-host:5432/flag_db"

	configFile := writeTempConfig(t, `postgres_dsn = "`+fileDSN+`"`)

	// Step 1: only the config file set -> file value wins (no default to fall back to).
	cfg, err := Load(Flags{ConfigFile: configFile})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresDSN != fileDSN {
		t.Fatalf("step 1: PostgresDSN = %q, want file value %q", cfg.PostgresDSN, fileDSN)
	}

	// Step 2: env var set alongside the file -> env wins over file.
	t.Setenv(envPostgresDSN, envDSN)
	cfg, err = Load(Flags{ConfigFile: configFile})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresDSN != envDSN {
		t.Fatalf("step 2: PostgresDSN = %q, want env value %q", cfg.PostgresDSN, envDSN)
	}

	// Step 3: flag set alongside both -> flag wins over env (and file).
	cfg, err = Load(Flags{ConfigFile: configFile, PostgresDSN: flagDSN})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresDSN != flagDSN {
		t.Fatalf("step 3: PostgresDSN = %q, want flag value %q", cfg.PostgresDSN, flagDSN)
	}
}

// TestLoad_ExplicitZeroIntFlagIsNotIgnored confirms the *int pointer trick for
// ConfirmationDepth/InvoiceTTLMinutes: an explicit 0 override must be honored, not
// treated as "flag not set" and silently replaced by the default.
func TestLoad_ExplicitZeroIntFlagIsNotIgnored(t *testing.T) {
	zero := 0
	cfg, err := Load(Flags{PostgresDSN: "postgres://example/db", ConfirmationDepth: &zero})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ConfirmationDepth != 0 {
		t.Errorf("ConfirmationDepth = %d, want 0 (explicit override must not be replaced by default)", cfg.ConfirmationDepth)
	}
}

// TestLoad_InvalidIntEnvVarErrors confirms a malformed integer env var is a hard error,
// not a silent fallback to the default.
func TestLoad_InvalidIntEnvVarErrors(t *testing.T) {
	t.Setenv(envConfirmationDepth, "not-a-number")
	_, err := Load(Flags{PostgresDSN: "postgres://example/db"})
	if err == nil {
		t.Fatal("Load() error = nil, want an error for a malformed TARIPAY_CONFIRMATION_DEPTH")
	}
}

// writeTempConfig writes contents to a temp TOML file and returns its path.
func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writeTempConfig: %v", err)
	}
	return path
}
