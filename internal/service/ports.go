package service

import (
	"context"
	"time"

	"github.com/nkhoit/agent-journal/internal/domain"
)

// JournalStore is the central durable boundary. Implementations must make
// record insertion and mailbox obligation creation one transaction.
type JournalStore interface {
	Append(ctx context.Context, actor, space string, input domain.RecordInput, idempotencyKey string) (domain.AppendResult, error)
	Get(ctx context.Context, actor, recordID string) (domain.Record, error)
	List(ctx context.Context, actor, space, cursor string, limit int) ([]domain.Record, string, error)
	Search(ctx context.Context, actor, space, query, order, cursor string, limit int) ([]domain.Record, string, error)
}

// Authorizer answers authorization decisions without exposing storage details.
type Authorizer interface {
	CanRead(ctx context.Context, principal, space string) (bool, error)
	CanAppend(ctx context.Context, principal, space string) (bool, error)
	// CanReadDeliveryStatus authorizes one recipient-scoped entry. An author
	// may call this for each recipient of a record they can read; an addressed
	// recipient may call it only for their own principal. Other space readers
	// must receive no status or existence signal.
	CanReadDeliveryStatus(ctx context.Context, principal, recordID, recipient string) (bool, error)
	CanAdmin(ctx context.Context, principal string) (bool, error)
}

// MailboxStore is intentionally separate from principal-client storage APIs.
type MailboxStore interface {
	Claim(ctx context.Context, request ClaimRequest) (Claim, error)
	Commit(ctx context.Context, request CommitRequest) (CommitResult, error)
	RecordEvent(ctx context.Context, request EventRequest) error
	// recipient is either the authenticated addressed principal for a single
	// entry or empty for an already-authorized record author requesting all
	// recipient entries. The service must authorize before calling this method.
	DeliveryStatus(ctx context.Context, recordID, recipient, cursor string, limit int) ([]DeliverySummary, string, error)
	Status(ctx context.Context, principal string) (MailboxStatus, error)
}

type ClaimRequest struct {
	Principal  string
	AdapterID  string
	InstanceID string
	Generation int64
	Limit      int
	Wait       time.Duration
}

type Claim struct {
	ID             string
	AttemptIDs     []string
	LeaseExpiresAt time.Time
}

type CommitItem struct {
	MailboxItemID string
	AttemptID     string
}

type CommitItemResult struct {
	MailboxItemID string
	AttemptID     string
	Result        string
}

type CommitRequest struct {
	ClaimID    string
	Generation int64
	Items      []CommitItem
}

type CommitResult struct {
	ClaimID    string
	Generation int64
	Items      []CommitItemResult
}

type EventRequest struct {
	// AdapterID, Principal, and InstanceID come from authenticated adapter
	// context; they are never accepted from the JSON event body.
	MailboxItemID string
	AttemptID     string
	EventID       string
	AdapterID     string
	Principal     string
	InstanceID    string
	Generation    int64
	State         domain.DeliveryState
	OccurredAt    time.Time
	Detail        map[string]string
}

func (r EventRequest) Validate() error {
	return domain.ValidateTelemetryDetail(r.Detail)
}

type DeliverySummary struct {
	MailboxItemID string
	Recipient     string
	State         domain.DeliveryState
	Attempts      int
	LastAttemptID string
	UpdatedAt     time.Time
}

type MailboxStatus struct {
	Pending int
	Oldest  time.Time
}

type Clock interface{ Now() time.Time }
type IDGenerator interface{ New() (string, error) }
