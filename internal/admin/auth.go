package admin

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// bearerPrefix is the standard HTTP Authorization scheme prefix this middleware
// expects: "Authorization: Bearer <token>".
const bearerPrefix = "Bearer "

// requireAuth wraps next with shared static bearer-token authentication — the S1/I19
// admin-auth fix (task brief "fix C2 and add auth", part 2). This is the
// simplest-correct fix for a self-hosted-per-merchant single-tenant service (per the
// brief's design decision): one shared secret gates every admin route
// (/admin, /admin/invoices, /admin/webhooks/{id}/retry), NOT the merchant-facing
// /invoice API, which the cart plugin must keep calling without a shared secret
// (out of scope here, see the brief).
//
// The provided token is compared against the expected token via
// crypto/subtle.ConstantTimeCompare rather than a plain string == comparison,
// specifically to avoid a timing side-channel on a secret comparison. token being
// empty is treated as an always-reject configuration error rather than "anything
// matches" — RegisterRoutes's caller (cmd/gateway/main.go) is expected to never wire
// this middleware up with an empty token in the first place (an unset
// AdminAuthToken means the admin routes aren't registered at all, see that package's
// doc comment), but failing closed here costs nothing and removes the assumption.
//
// On a missing/malformed Authorization header or a token mismatch, responds 401 with
// a WWW-Authenticate header (per RFC 7235) and does not call next at all.
func requireAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validBearerToken(token, r.Header.Get("Authorization")) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="taripay-admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validBearerToken reports whether authHeader is a well-formed "Bearer <token>"
// header whose token matches expectedToken, using a constant-time comparison.
func validBearerToken(expectedToken, authHeader string) bool {
	if expectedToken == "" {
		// Fail closed: an empty expected token is a configuration error, never
		// a "no auth required" signal — see this file's doc comment.
		return false
	}
	provided, ok := strings.CutPrefix(authHeader, bearerPrefix)
	if !ok {
		return false
	}
	// subtle.ConstantTimeCompare requires equal-length inputs to be meaningful
	// (it returns 0 immediately on a length mismatch, which is fine — that
	// only leaks the *length* of the provided token, not anything about
	// whether any of its bytes are correct). Padding isn't needed here: a
	// length mismatch alone already returns 0/false with no further
	// comparison, in constant time relative to the *content* comparison,
	// which is all that matters for a secret-equality check.
	return len(provided) == len(expectedToken) &&
		subtle.ConstantTimeCompare([]byte(provided), []byte(expectedToken)) == 1
}
