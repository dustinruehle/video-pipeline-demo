// Package tclient builds a Temporal client configured for either the local
// dev server (default) or Temporal Cloud, driven by TEMPORAL_TARGET.
package tclient

import (
	"crypto/tls"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"
)

// New returns a Temporal client.
//
// TEMPORAL_TARGET defaults to "dev". When "cloud" is set, TEMPORAL_ADDRESS,
// TEMPORAL_NAMESPACE, and TEMPORAL_API_KEY are required and TLS + API-key
// auth are configured. mTLS is intentionally not built; document any
// fallback in NOTES.md if API key auth fails on demo day.
func New() (client.Client, error) {
	target := os.Getenv("TEMPORAL_TARGET")
	if target == "" {
		target = "dev"
	}

	switch target {
	case "dev":
		address := os.Getenv("TEMPORAL_ADDRESS")
		if address == "" {
			address = "localhost:7233"
		}
		ns := os.Getenv("TEMPORAL_NAMESPACE")
		if ns == "" {
			ns = "default"
		}
		return client.Dial(client.Options{
			HostPort:  address,
			Namespace: ns,
		})

	case "cloud":
		address := os.Getenv("TEMPORAL_ADDRESS")
		if address == "" {
			return nil, fmt.Errorf("TEMPORAL_ADDRESS required for cloud target")
		}
		ns := os.Getenv("TEMPORAL_NAMESPACE")
		if ns == "" {
			return nil, fmt.Errorf("TEMPORAL_NAMESPACE required for cloud target")
		}
		apiKey := os.Getenv("TEMPORAL_API_KEY")
		if apiKey == "" {
			return nil, fmt.Errorf("TEMPORAL_API_KEY required for cloud target")
		}
		return client.Dial(client.Options{
			HostPort:  address,
			Namespace: ns,
			ConnectionOptions: client.ConnectionOptions{
				TLS: &tls.Config{},
			},
			Credentials: client.NewAPIKeyStaticCredentials(apiKey),
		})

	default:
		return nil, fmt.Errorf("unknown TEMPORAL_TARGET=%q (expected dev or cloud)", target)
	}
}

// UIBaseURL returns the base URL for the workflow list in the Temporal UI,
// matching scripts/lib.sh ui_url(). Used by starters when printing links.
func UIBaseURL() string {
	target := os.Getenv("TEMPORAL_TARGET")
	if target == "cloud" {
		return fmt.Sprintf("https://cloud.temporal.io/namespaces/%s/workflows", os.Getenv("TEMPORAL_NAMESPACE"))
	}
	return "http://localhost:8233/namespaces/default/workflows"
}
