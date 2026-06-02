package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// genCertPEM mints a short-lived self-signed certificate for dnsName with the
// given serial and returns its PEM cert + key plus the parsed cert.
func genCertPEM(t *testing.T, dnsName string, serial int64) (certPEM, keyPEM []byte, parsed *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: dnsName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{dnsName},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	parsed, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, parsed
}

// writeCertTo writes a freshly-minted keypair (given serial) to the supplied
// paths — the tls.crt / tls.key shape cert-manager projects into the pod.
func writeCertTo(t *testing.T, certFile, keyFile, dnsName string, serial int64) {
	t.Helper()
	certPEM, keyPEM, _ := genCertPEM(t, dnsName, serial)
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
}

// writeTestCert mints a cert for dnsName into a temp dir and returns its paths
// plus a CA pool that trusts it so a client can verify the real chain — not
// just skip verification.
func writeTestCert(t *testing.T, dnsName string) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	certPEM, keyPEM, parsed := genCertPEM(t, dnsName, 1)
	dir := t.TempDir()
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(parsed)
	return certFile, keyFile, pool
}

// TestReloadingKeypairPicksUpRotation asserts the server re-reads the keypair
// when cert-manager rotates the mounted Secret (file rewritten in place), so a
// renewed cert is served without a pod restart.
func TestReloadingKeypairPicksUpRotation(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")

	writeCertTo(t, certFile, keyFile, "localhost", 1)
	rk := &reloadingKeypair{certFile: certFile, keyFile: keyFile}
	if err := rk.reload(); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	c1, err := rk.getCertificate(nil)
	if err != nil {
		t.Fatalf("getCertificate (1): %v", err)
	}
	leaf1, err := x509.ParseCertificate(c1.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf (1): %v", err)
	}
	if leaf1.SerialNumber.Int64() != 1 {
		t.Fatalf("serial = %d, want 1", leaf1.SerialNumber.Int64())
	}

	// Rotate the files in place and advance the mtime (a fast test can write
	// twice within one mtime tick, so set it explicitly).
	writeCertTo(t, certFile, keyFile, "localhost", 2)
	future := time.Now().Add(time.Minute)
	if err := os.Chtimes(certFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	c2, err := rk.getCertificate(nil)
	if err != nil {
		t.Fatalf("getCertificate (2): %v", err)
	}
	leaf2, err := x509.ParseCertificate(c2.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf (2): %v", err)
	}
	if leaf2.SerialNumber.Int64() != 2 {
		t.Fatalf("expected reloaded cert serial 2 after rotation, got %d", leaf2.SerialNumber.Int64())
	}
}

func TestLoadGRPCTLSCredentials(t *testing.T) {
	certFile, keyFile, _ := writeTestCert(t, "localhost")

	t.Run("both empty serves plaintext", func(t *testing.T) {
		creds, err := LoadGRPCTLSCredentials("", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if creds != nil {
			t.Fatalf("expected nil creds (plaintext) when both paths empty, got %v", creds)
		}
	})

	t.Run("cert without key fails", func(t *testing.T) {
		_, err := LoadGRPCTLSCredentials(certFile, "")
		if !errors.Is(err, ErrTLSPartialConfig) {
			t.Fatalf("expected ErrTLSPartialConfig, got %v", err)
		}
	})

	t.Run("key without cert fails", func(t *testing.T) {
		_, err := LoadGRPCTLSCredentials("", keyFile)
		if !errors.Is(err, ErrTLSPartialConfig) {
			t.Fatalf("expected ErrTLSPartialConfig, got %v", err)
		}
	})

	t.Run("both set loads creds", func(t *testing.T) {
		creds, err := LoadGRPCTLSCredentials(certFile, keyFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if creds == nil {
			t.Fatal("expected non-nil creds")
		}
	})

	t.Run("both set but unreadable is not partial-config", func(t *testing.T) {
		_, err := LoadGRPCTLSCredentials(filepath.Join(t.TempDir(), "missing.crt"), filepath.Join(t.TempDir(), "missing.key"))
		if err == nil {
			t.Fatal("expected a load error for missing files")
		}
		if errors.Is(err, ErrTLSPartialConfig) {
			t.Fatalf("missing-file error must not be ErrTLSPartialConfig, got %v", err)
		}
	})
}

// startTLSServer starts a Service with the given gRPC credentials on real
// loopback TCP listeners and returns the gRPC address + public HTTP base URL +
// a stop func. When creds is nil the gRPC listener is plaintext.
func startTLSServer(t *testing.T, creds credentials.TransportCredentials) (grpcAddr, httpBaseURL string, stop func()) {
	t.Helper()
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = grpcListener.Close()
		t.Fatalf("listen http: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = grpcListener.Close()
		_ = httpListener.Close()
		t.Fatalf("listen snapshot: %v", err)
	}

	var opts []Option
	if creds != nil {
		opts = append(opts, WithGRPCTLS(creds))
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(opts...).Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()

	stop = func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}
	return grpcListener.Addr().String(), "http://" + httpListener.Addr().String(), stop
}

// TestServeGRPCOverTLS asserts a TLS-configured server presents a verifiable
// cert, answers the standard health check + a fail-open LookupRoute over TLS,
// and reports the posture via inferencecache_server_grpc_tls_enabled.
func TestServeGRPCOverTLS(t *testing.T) {
	certFile, keyFile, pool := writeTestCert(t, "localhost")
	creds, err := LoadGRPCTLSCredentials(certFile, keyFile)
	if err != nil {
		t.Fatalf("load creds: %v", err)
	}
	grpcAddr, httpBaseURL, stop := startTLSServer(t, creds)
	defer stop()

	clientTLS := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost"})
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(clientTLS))
	if err != nil {
		t.Fatalf("dial tls: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hc := healthpb.NewHealthClient(conn)
	hResp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check over TLS: %v", err)
	}
	if hResp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %v, want SERVING", hResp.GetStatus())
	}

	// The application service must also answer over TLS (fail-open default).
	lr, err := icpb.NewInferenceCacheClient(conn).LookupRoute(ctx, &icpb.LookupRouteRequest{ModelId: "unknown"})
	if err != nil {
		t.Fatalf("LookupRoute over TLS: %v", err)
	}
	if lr.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason_code = %q, want NO_HINT", lr.GetReasonCode())
	}

	code, body := getString(t, httpBaseURL+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics status = %d", code)
	}
	if !strings.Contains(body, "inferencecache_server_grpc_tls_enabled 1") {
		t.Fatalf("expected grpc_tls_enabled gauge = 1 in /metrics, got:\n%s", body)
	}
}

// TestServeGRPCPlaintext asserts the default (no creds) listener serves
// plaintext and reports the posture gauge as 0.
func TestServeGRPCPlaintext(t *testing.T) {
	grpcAddr, httpBaseURL, stop := startTLSServer(t, nil)
	defer stop()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial plaintext: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check over plaintext: %v", err)
	}

	code, body := getString(t, httpBaseURL+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics status = %d", code)
	}
	if !strings.Contains(body, "inferencecache_server_grpc_tls_enabled 0") {
		t.Fatalf("expected grpc_tls_enabled gauge = 0 in /metrics, got:\n%s", body)
	}
}

// TestPlaintextClientRejectedByTLSServer is the negative case the kind smoke
// asserts at the cluster level: a plaintext client cannot talk to the TLS
// listener (the handshake fails), so an accidental plaintext deployment can't
// silently downgrade.
func TestPlaintextClientRejectedByTLSServer(t *testing.T) {
	certFile, keyFile, _ := writeTestCert(t, "localhost")
	creds, err := LoadGRPCTLSCredentials(certFile, keyFile)
	if err != nil {
		t.Fatalf("load creds: %v", err)
	}
	grpcAddr, _, stop := startTLSServer(t, creds)
	defer stop()

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err == nil {
		t.Fatal("expected plaintext client to be rejected by the TLS server, got nil error")
	}
}
