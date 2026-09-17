package spool

import (
	"context"
	"errors"

	adaptercore "github.com/nkhoit/agent-journal/internal/adapter/core"
)

var ErrNotReady = errors.New("durable spool not implemented")

// Store is the local custody boundary. A real implementation must use a
// local transactional store and fsync the complete SpoolItem before returning
// from Put. It must persist claim_id, instance_id, generation, custody
// confirmation, injection state, receipt, and retry metadata, implement the
// idempotent transition methods from adaptercore.Spool, and make Recoverable
// safe to call after a process crash. The scaffold deliberately has no
// database-backed implementation yet.
type Store interface {
	adaptercore.Spool
	Open(ctx context.Context) error
	Close() error
}
