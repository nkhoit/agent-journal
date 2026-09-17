package journalclient

import (
	"context"
	"errors"
)

var ErrUnavailable = errors.New("journal service unavailable")

// Transport is the seam for the future authenticated HTTP client.
type Transport interface {
	Do(ctx context.Context, method, path string, body []byte, headers map[string]string) (status int, response []byte, err error)
}

// Client will own request IDs, idempotency, bounded pagination, and safe error
// decoding. The scaffold deliberately has no network implementation yet.
type Client struct{ transport Transport }

func New(transport Transport) *Client { return &Client{transport: transport} }

func (c *Client) TransportConfigured() bool { return c != nil && c.transport != nil }
