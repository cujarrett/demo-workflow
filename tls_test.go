package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeCert writes a self-signed key pair with the given serial, as spiffe-helper would.
func writeCert(t *testing.T, certFile, keyFile string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "foo"},
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func certSerial(t *testing.T, cfg *tls.Config) int64 {
	t.Helper()
	cert, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return parsed.SerialNumber.Int64()
}

func TestTLSConfigIsOffWithoutCertFiles(t *testing.T) {
	if cfg := newTLSConfig("", ""); cfg != nil {
		t.Fatal("private-cloud and local runs must connect without client TLS")
	}
}

func TestTLSConfigPicksUpARotatedCert(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "svid.pem"), filepath.Join(dir, "svid_key.pem")

	writeCert(t, certFile, keyFile, 1)
	cfg := newTLSConfig(certFile, keyFile)
	if got := certSerial(t, cfg); got != 1 {
		t.Fatalf("serial = %d, want 1", got)
	}

	writeCert(t, certFile, keyFile, 2)
	if got := certSerial(t, cfg); got != 2 {
		t.Fatalf("serial = %d, want 2 after rotation", got)
	}
}

// The cert files appear shortly after start, so a miss must fail the handshake and let dial retry.
func TestTLSConfigFailsUntilTheFilesExist(t *testing.T) {
	dir := t.TempDir()
	cfg := newTLSConfig(filepath.Join(dir, "svid.pem"), filepath.Join(dir, "svid_key.pem"))
	if _, err := cfg.GetClientCertificate(nil); err == nil {
		t.Fatal("want an error while the files are missing")
	}
}
