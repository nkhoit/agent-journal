package storage

import (
	"context"
	"time"
)

// Tx is the minimal transaction boundary needed by the service layer.
type Tx interface {
	Commit() error
	Rollback() error
}

// Transactioner keeps transaction ownership explicit at call sites.
type Transactioner interface {
	WithTx(ctx context.Context, fn func(Tx) error) error
}

// BackupSource is implemented by the future SQLite repository. Backups are a
// recovery primitive and are not part of the public record protocol.
type BackupSource interface {
	Backup(ctx context.Context, destination string) error
	LastBackupAt(ctx context.Context) (time.Time, error)
}
