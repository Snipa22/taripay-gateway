// Command healthcheck is a tiny, standalone binary whose only job is to hit
// cmd/gateway's GET /health endpoint and exit 0 (healthy) or 1 (unhealthy) — built
// specifically for docker/gateway/Dockerfile's HEALTHCHECK directive (task brief
// part 4, I13/I27).
//
// This exists because the runtime image is gcr.io/distroless/static-debian12:nonroot,
// which ships no shell and no curl/wget at all — Docker's HEALTHCHECK CMD normally
// shells out to a tool like curl, but that's not an option here, so this repo
// compiles its own minimal HTTP-GET-and-check-status-code binary alongside
// cmd/gateway instead, and the Dockerfile invokes it directly via HEALTHCHECK CMD's
// exec form (no shell required either way).
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

// defaultHealthURL matches cmd/gateway's config.DefaultHTTPListenAddr (":8090")
// resolved against localhost, since this binary always runs inside the same
// container as cmd/gateway. Overridable via -url or TARIPAY_HEALTHCHECK_URL for a
// non-default TARIPAY_HTTP_LISTEN_ADDR — same "flag > env var > default"
// resolution-order convention as internal/config, kept intentionally tiny here
// rather than pulling that whole package into this small a binary.
const defaultHealthURL = "http://127.0.0.1:8090/health"

// requestTimeout bounds how long this binary will wait on the health check
// request before treating it as a failure — a health check that itself hangs
// forever defeats the entire point of a HEALTHCHECK directive. 5s is a
// PLACEHOLDER, UNCONFIRMED value, same caveat as cmd/gateway's other tunables.
const requestTimeout = 5 * time.Second

func main() {
	url := flag.String("url", "", "URL to GET for the health check (env: TARIPAY_HEALTHCHECK_URL; default: "+defaultHealthURL+")")
	flag.Parse()

	target := *url
	if target == "" {
		target = os.Getenv("TARIPAY_HEALTHCHECK_URL")
	}
	if target == "" {
		target = defaultHealthURL
	}

	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Get(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: GET %s: %v\n", target, err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: GET %s: status = %d, want %d\n", target, resp.StatusCode, http.StatusOK)
		os.Exit(1)
	}
}
