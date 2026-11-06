package main

import "crypto/tls"

// newTLSConfig returns nil when no cert files are set, so private-cloud and local runs skip client TLS.
//
// The files are read on every handshake.
//   - Rotation: spiffe-helper replaces them, so a new connection uses the current SVID.
//   - Startup: the files can be missing at first, so the handshake fails and dial retries.
func newTLSConfig(certFile, keyFile string) *tls.Config {
	if certFile == "" || keyFile == "" {
		return nil
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}
}
