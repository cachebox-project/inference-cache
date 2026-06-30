package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(scheme()).WithObjects(objs...).Build()
}

func svc(ns, name string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func TestStripScheme(t *testing.T) {
	cases := map[string]string{
		"lm://host:1":    "host:1",
		"http://host:2":  "host:2",
		"https://host:3": "host:3",
		"host:4":         "host:4",
		"[::1]:5":        "[::1]:5",
	}
	for in, want := range cases {
		if got := stripScheme(in); got != want {
			t.Errorf("stripScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReadToken(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		if got := readToken(""); got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		if got := readToken(filepath.Join(t.TempDir(), "nope")); got != "" {
			t.Errorf("missing file should yield empty, got %q", got)
		}
	})
	t.Run("trims trailing whitespace", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(p, []byte("  tok-value\n\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := readToken(p); got != "tok-value" {
			t.Errorf("got %q, want trimmed tok-value", got)
		}
	})
	t.Run("unreadable (directory) yields empty", func(t *testing.T) {
		// os.ReadFile on a directory returns a non-NotExist error → empty + warning.
		if got := readToken(t.TempDir()); got != "" {
			t.Errorf("unreadable path should yield empty, got %q", got)
		}
	})
}

func TestResolveEndpoints(t *testing.T) {
	ctx := context.Background()

	t.Run("override host only defaults the gRPC port", func(t *testing.T) {
		g, snap, pol, probe, err := resolveEndpoints(ctx, newFakeClient(t), "localhost")
		if err != nil {
			t.Fatal(err)
		}
		if g != "localhost:9090" {
			t.Errorf("grpc = %q", g)
		}
		if snap != "http://localhost:8081/snapshot" || pol != "http://localhost:8081/policy" || probe != "http://localhost:8081/probe" {
			t.Errorf("http urls: %q %q %q", snap, pol, probe)
		}
	})

	t.Run("override host:port overrides the gRPC port, HTTP stays 8081", func(t *testing.T) {
		g, snap, _, _, err := resolveEndpoints(ctx, newFakeClient(t), "host.example:19090")
		if err != nil {
			t.Fatal(err)
		}
		if g != "host.example:19090" {
			t.Errorf("grpc = %q", g)
		}
		if snap != "http://host.example:8081/snapshot" {
			t.Errorf("snapshot = %q", snap)
		}
	})

	t.Run("discovers the Service in the system namespace", func(t *testing.T) {
		c := newFakeClient(t, svc(defaultSystemNamespace, defaultServerService))
		g, snap, _, _, err := resolveEndpoints(ctx, c, "")
		if err != nil {
			t.Fatal(err)
		}
		wantHost := defaultServerService + "." + defaultSystemNamespace + ".svc"
		if g != wantHost+":9090" {
			t.Errorf("grpc = %q, want host %q", g, wantHost)
		}
		if snap != "http://"+wantHost+":8081/snapshot" {
			t.Errorf("snapshot = %q", snap)
		}
	})

	t.Run("no Service anywhere is an error", func(t *testing.T) {
		if _, _, _, _, err := resolveEndpoints(ctx, newFakeClient(t), ""); err == nil {
			t.Fatal("want error when no Service is discoverable")
		}
	})
}

func TestFindServerServiceNamespace(t *testing.T) {
	ctx := context.Background()

	t.Run("found in system namespace", func(t *testing.T) {
		c := newFakeClient(t, svc(defaultSystemNamespace, defaultServerService))
		ns, err := findServerServiceNamespace(ctx, c)
		if err != nil || ns != defaultSystemNamespace {
			t.Fatalf("ns=%q err=%v", ns, err)
		}
	})

	t.Run("found via cluster-wide search in another namespace", func(t *testing.T) {
		c := newFakeClient(t, svc("other-ns", defaultServerService))
		ns, err := findServerServiceNamespace(ctx, c)
		if err != nil || ns != "other-ns" {
			t.Fatalf("ns=%q err=%v", ns, err)
		}
	})

	t.Run("absent is an error", func(t *testing.T) {
		if _, err := findServerServiceNamespace(ctx, newFakeClient(t)); err == nil {
			t.Fatal("want error when absent")
		}
	})

	t.Run("a non-NotFound Get error is surfaced, not masked", func(t *testing.T) {
		c := getErrClient{Client: newFakeClient(t), err: errors.New("forbidden")}
		if _, err := findServerServiceNamespace(ctx, c); err == nil {
			t.Fatal("a Forbidden Get must surface, not fall through to a cluster search")
		}
	})
}

// getErrClient forces every Get to fail with a non-NotFound error.
type getErrClient struct {
	client.Client
	err error
}

func (c getErrClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return c.err
}
