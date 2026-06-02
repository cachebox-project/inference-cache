package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

// ErrTLSPartialConfig is returned when exactly one of the cert/key paths is
// supplied. TLS is all-or-nothing: a cert without a key (or vice versa) is an
// operator mistake, not a half-configured mode we can silently resolve, so we
// fail loudly at startup rather than guess. Exported so cmd/server can map it
// to a flag-usage exit code (and tests can assert on it via errors.Is).
var ErrTLSPartialConfig = errors.New(
	"server: --tls-cert-file and --tls-key-file must both be set or both be empty")

// LoadGRPCTLSCredentials resolves the gRPC transport posture from a cert/key
// file pair:
//
//   - both empty   → (nil, nil): the caller serves plaintext (the default;
//     TLS is the opt-in config/overlays/server-tls overlay).
//   - both set     → load the keypair and return server TLS credentials that
//     reload the keypair from disk when it changes (cert-manager rotation).
//   - exactly one  → (nil, ErrTLSPartialConfig): refuse to start.
//
// Keeping the both-or-neither rule here (rather than inline in cmd/server)
// makes it unit-testable and keeps the single source of truth for the posture
// decision. See docs/design/grpc-tls.md.
func LoadGRPCTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	switch {
	case certFile == "" && keyFile == "":
		return nil, nil
	case certFile == "" || keyFile == "":
		return nil, ErrTLSPartialConfig
	}
	rk := &reloadingKeypair{certFile: certFile, keyFile: keyFile}
	// Load once up front so a bad/unreadable keypair fails the server at
	// startup rather than on the first client handshake.
	if err := rk.reload(); err != nil {
		return nil, fmt.Errorf("server: load gRPC TLS keypair (cert=%q key=%q): %w", certFile, keyFile, err)
	}
	return credentials.NewTLS(&tls.Config{
		GetCertificate: rk.getCertificate,
		MinVersion:     tls.VersionTLS12,
	}), nil
}

// reloadingKeypair serves the gRPC cert via tls.Config.GetCertificate and
// re-reads the cert/key files when the cert file's mtime advances. The default
// install mounts a cert-manager-managed Secret, which cert-manager renews well
// before expiry (default ~2/3 of lifetime) by rewriting the Secret — kubelet
// then atomically swaps the projected files. Without reloading, the in-memory
// cert would go stale until the pod restarted and could eventually be served
// past expiry. Detecting via mtime keeps the steady-state handshake path off
// disk except right after a rotation.
type reloadingKeypair struct {
	certFile, keyFile string

	mu      sync.RWMutex
	cached  *tls.Certificate
	modTime time.Time
}

// getCertificate is the tls.Config.GetCertificate hook. It reloads first when
// the cert file changed, then returns the cached keypair. On a reload error
// (e.g. a torn read during rotation) it logs and serves the last-good cert —
// failing soft beats dropping TLS for a transient filesystem hiccup.
func (r *reloadingKeypair) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if info, err := os.Stat(r.certFile); err == nil {
		r.mu.RLock()
		changed := info.ModTime().After(r.modTime)
		r.mu.RUnlock()
		if changed {
			if rerr := r.reload(); rerr != nil {
				slog.Error("grpc_tls_reload", "err", rerr, "cert_file", r.certFile)
			}
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.cached == nil {
		return nil, errors.New("server: no gRPC TLS certificate loaded")
	}
	return r.cached, nil
}

// reload reads the keypair from disk and atomically swaps the cache + recorded
// mtime under the write lock.
func (r *reloadingKeypair) reload() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return err
	}
	var mod time.Time
	if info, statErr := os.Stat(r.certFile); statErr == nil {
		mod = info.ModTime()
	}
	r.mu.Lock()
	r.cached = &cert
	r.modTime = mod
	r.mu.Unlock()
	return nil
}

// WithGRPCTLS wires server-side TLS credentials onto the gRPC listener. When
// the option is absent (or creds is nil), the gRPC server serves plaintext.
// New reads this back to set the inferencecache_server_grpc_tls_enabled gauge.
// Tests inject credentials directly; the production binary loads them from the
// flag-supplied files via LoadGRPCTLSCredentials.
func WithGRPCTLS(creds credentials.TransportCredentials) Option {
	return func(s *Service) {
		s.grpcCreds = creds
	}
}
