// Package config resolves this repo's process-wide configuration: the wallet gRPC
// address, the Postgres DSN, the HTTP listen address, the confirmation-depth
// threshold, and the invoice TTL.
//
// Resolution order for every field is CLI flag > env var > config file > default,
// following the pattern used by sibling repo go-tari-ootle-explorer's
// internal/config/config.go (see that package's doc comment) — AGENTS.md requires
// this repo match that precedent rather than inventing a new resolution order.
//
// Per AGENTS.md's "no static/hardcoded assumptions" rule, every field here is
// config-driven; the only genuine defaults are ones documented and cited below (either
// as a real upstream default, or explicitly flagged as an unconfirmed placeholder that
// the human project owner has not yet locked in).
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/pelletier/go-toml/v2"
)

// DefaultWalletGRPCAddress matches minotari_console_wallet's own documented default
// gRPC bind address (grpc_address = "/ip4/127.0.0.1/tcp/18143" in the wallet's default
// config preset, common/config/presets/c_wallet.toml in tari-project/tari) — i.e. this
// is "point at a minotari_console_wallet started with no config overrides on this
// machine", not an arbitrary made-up port.
const DefaultWalletGRPCAddress = "127.0.0.1:18143"

// DefaultHTTPListenAddr is the address cmd/gateway listens on absent an override.
const DefaultHTTPListenAddr = ":8090"

// DefaultConfirmationDepth is a PLACEHOLDER, UNCONFIRMED default. It has not been
// validated against actual Tari testnet/mainnet block-time characteristics by the
// project owner (Alex) — it is a reasonable-sounding guess, nothing more. Override via
// TARIPAY_CONFIRMATION_DEPTH (or the -confirmation-depth flag) until a real value is
// set; do not treat this constant as settled guidance.
const DefaultConfirmationDepth = 3

// DefaultInvoiceTTLMinutes is a PLACEHOLDER, UNCONFIRMED default, same caveat as
// DefaultConfirmationDepth above: not yet confirmed with the project owner. Override
// via TARIPAY_INVOICE_TTL_MINUTES (or the -invoice-ttl-minutes flag) until a real value
// is set.
const DefaultInvoiceTTLMinutes = 30

// Env var names, following this org's <PROJECT>_<FIELD> convention (see
// go-tari-ootle-explorer's TARI_OOTLE_EXPLORER_* / this repo's TARIPAY_* prefix).
const (
	envConfigFile        = "TARIPAY_CONFIG_FILE"
	envWalletGRPCAddress = "TARIPAY_WALLET_GRPC_ADDR"
	envPostgresDSN       = "TARIPAY_POSTGRES_DSN"
	envHTTPListenAddr    = "TARIPAY_HTTP_LISTEN_ADDR"
	envConfirmationDepth = "TARIPAY_CONFIRMATION_DEPTH"
	envInvoiceTTLMinutes = "TARIPAY_INVOICE_TTL_MINUTES"
)

// Config is the fully-resolved, ready-to-use configuration.
type Config struct {
	WalletGRPCAddress string
	PostgresDSN       string
	HTTPListenAddr    string
	ConfirmationDepth int
	InvoiceTTLMinutes int
}

// Flags holds every CLI-flag-shaped override this package accepts. Callers (cmd/*
// binaries) populate this from their own flag.FlagSet after parsing; this package
// deliberately doesn't own flag registration, so it stays testable without any actual
// command-line plumbing.
//
// ConfirmationDepth/InvoiceTTLMinutes are pointers rather than plain ints so an unset
// flag (zero value 0) can be distinguished from an explicit "0" override — a plain int
// would make "flag not passed" indistinguishable from "flag explicitly set to 0" and
// therefore always win over env/file/default.
type Flags struct {
	ConfigFile        string
	WalletGRPCAddress string
	PostgresDSN       string
	HTTPListenAddr    string
	ConfirmationDepth *int
	InvoiceTTLMinutes *int
}

// fileConfig is the raw shape decoded from an optional TOML config file. A config file
// is only read if Flags.ConfigFile or TARIPAY_CONFIG_FILE is set — there is no implicit
// default config file path, per AGENTS.md's rule against hardcoded/implicit infra
// assumptions.
type fileConfig struct {
	WalletGRPCAddress string `toml:"wallet_grpc_address"`
	PostgresDSN       string `toml:"postgres_dsn"`
	HTTPListenAddr    string `toml:"http_listen_addr"`
	ConfirmationDepth *int   `toml:"confirmation_depth"`
	InvoiceTTLMinutes *int   `toml:"invoice_ttl_minutes"`
}

// Load resolves a Config from flags, env vars, an optional TOML config file, and this
// package's defaults, in that precedence order for every field.
//
// PostgresDSN has no real default (see AGENTS.md: "must error clearly if unset in a
// non-test context") — if it resolves to empty after checking every source, Load
// returns an error rather than silently falling back to some hardcoded connection
// string that would almost certainly point at the wrong database.
func Load(flags Flags) (*Config, error) {
	var file fileConfig

	configPath := firstNonEmpty(flags.ConfigFile, os.Getenv(envConfigFile))
	if configPath != "" {
		raw, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("config: read %s: %w", configPath, err)
		}
		if err := toml.Unmarshal(raw, &file); err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", configPath, err)
		}
	}

	walletAddr := firstNonEmpty(flags.WalletGRPCAddress, os.Getenv(envWalletGRPCAddress), file.WalletGRPCAddress, DefaultWalletGRPCAddress)
	httpAddr := firstNonEmpty(flags.HTTPListenAddr, os.Getenv(envHTTPListenAddr), file.HTTPListenAddr, DefaultHTTPListenAddr)

	postgresDSN := firstNonEmpty(flags.PostgresDSN, os.Getenv(envPostgresDSN), file.PostgresDSN)
	if postgresDSN == "" {
		return nil, fmt.Errorf(
			"config: postgres DSN is required — set the -postgres-dsn flag, %s, or postgres_dsn in a config file (no default is provided; see AGENTS.md)",
			envPostgresDSN,
		)
	}

	confirmationDepth, err := firstNonZeroIntWithEnv(flags.ConfirmationDepth, envConfirmationDepth, file.ConfirmationDepth, DefaultConfirmationDepth)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", envConfirmationDepth, err)
	}

	invoiceTTLMinutes, err := firstNonZeroIntWithEnv(flags.InvoiceTTLMinutes, envInvoiceTTLMinutes, file.InvoiceTTLMinutes, DefaultInvoiceTTLMinutes)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", envInvoiceTTLMinutes, err)
	}

	return &Config{
		WalletGRPCAddress: walletAddr,
		PostgresDSN:       postgresDSN,
		HTTPListenAddr:    httpAddr,
		ConfirmationDepth: confirmationDepth,
		InvoiceTTLMinutes: invoiceTTLMinutes,
	}, nil
}

// firstNonEmpty returns the first non-empty string in vals, or "" if all are empty.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// firstNonZeroIntWithEnv resolves an int field in flag > env > file > default order.
// flagVal and fileVal are pointers so "not set" (nil) is distinguishable from an
// explicit 0. envName is parsed with strconv.Atoi if non-empty; a malformed env var
// value is a hard error rather than a silent fallback to the default.
func firstNonZeroIntWithEnv(flagVal *int, envName string, fileVal *int, def int) (int, error) {
	if flagVal != nil {
		return *flagVal, nil
	}
	if raw := os.Getenv(envName); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid integer %q: %w", raw, err)
		}
		return v, nil
	}
	if fileVal != nil {
		return *fileVal, nil
	}
	return def, nil
}
