package main

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// tokenHeaders adds an Entra access token to every Temporal call.
// The platform's sidecar keeps the federated token file fresh, and the Azure SDK
// trades it for an access token and caches it until shortly before it expires.
type tokenHeaders struct {
	cred  azcore.TokenCredential
	scope string
}

// headersProvider matches the Temporal SDK's unexported HeadersProvider interface.
type headersProvider interface {
	GetHeaders(ctx context.Context) (map[string]string, error)
}

// newTokenHeaders returns nil when no scope is set, so a local dev server needs no token.
func newTokenHeaders(scope string) headersProvider {
	if scope == "" {
		return nil
	}
	cred, err := azidentity.NewWorkloadIdentityCredential(nil) // reads the AZURE_* env vars
	if err != nil {
		return errorHeaders{err}
	}
	return tokenHeaders{cred: cred, scope: scope}
}

func (t tokenHeaders) GetHeaders(ctx context.Context) (map[string]string, error) {
	tok, err := t.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{t.scope}})
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + tok.Token}, nil
}

// errorHeaders fails every call with the setup error, so the pod logs why it cannot connect.
type errorHeaders struct{ err error }

func (e errorHeaders) GetHeaders(context.Context) (map[string]string, error) { return nil, e.err }
