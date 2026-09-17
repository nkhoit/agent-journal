package adaptercore

import (
	"context"
	"time"

	"github.com/nkhoit/agent-journal/internal/domain"
)

// Journal is the minimum central protocol surface for an adapter.
type Journal interface {
	Register(ctx context.Context, request RegisterRequest) (Registration, error)
	Heartbeat(ctx context.Context, request HeartbeatRequest) (Registration, error)
	Claim(ctx context.Context, request ClaimRequest) (ClaimBatch, error)
	CommitHostCustody(ctx context.Context, request CustodyRequest) (CustodyResult, error)
	RecordEvent(ctx context.Context, request EventRequest) error
}

// Spool is durable local host custody. Put must fsync the complete envelope
// before the adapter calls CommitHostCustody. Every transition is idempotent
// for the same attempt and binding; implementations must reject a conflicting
// claim, installation, generation, or receipt rather than overwrite history.
type Spool interface {
	Put(ctx context.Context, item SpoolItem) error
	Get(ctx context.Context, attemptID string) (SpoolItem, error)
	ConfirmCustody(ctx context.Context, attemptID, claimID, instanceID string, generation int64) error
	MarkInjectionStarted(ctx context.Context, attemptID, instanceID string, generation int64) error
	MarkInjected(ctx context.Context, attemptID, instanceID string, generation int64, receipt string) error
	MarkInjectionFailed(ctx context.Context, attemptID, instanceID string, generation int64, state InjectionState, detail string) error
	Recoverable(ctx context.Context, now time.Time, limit int) ([]SpoolItem, error)
}

// Runtime is vendor-specific and must not receive central credentials.
type Runtime interface {
	Inject(ctx context.Context, route Route, envelope Envelope) (receipt string, err error)
}

type RouteResolver interface {
	Resolve(space, routingKey string) (Route, error)
}

type RegisterRequest struct {
	InstanceID string
}

type HeartbeatRequest struct {
	InstanceID string
	Generation int64
}

type Registration struct {
	AdapterID             string
	PrincipalID           string
	InstanceID            string
	Generation            int64
	LeaseExpiresAt        string
	HeartbeatAfterSeconds int
}

type ClaimRequest struct {
	AdapterID  string
	InstanceID string
	Generation int64
	Limit      int
}

type ClaimBatch struct {
	ClaimID        string
	LeaseExpiresAt string
	Items          []SpoolItem
}

type CustodyItem struct {
	MailboxItemID string
	AttemptID     string
}

type CustodyItemResult struct {
	MailboxItemID string
	AttemptID     string
	Result        string
}

type CustodyRequest struct {
	ClaimID    string
	Generation int64
	Items      []CustodyItem
}

type CustodyResult struct {
	ClaimID    string
	Generation int64
	Items      []CustodyItemResult
}

type EventRequest struct {
	// AdapterID, Principal, and InstanceID are authentication-derived and are
	// not decoded from the event JSON body.
	MailboxItemID string
	AttemptID     string
	EventID       string
	AdapterID     string
	Principal     string
	InstanceID    string
	Generation    int64
	OccurredAt    string
	State         domain.DeliveryState
	Detail        map[string]string
}

func (r EventRequest) Validate() error {
	return domain.ValidateTelemetryDetail(r.Detail)
}

type InjectionState string

const (
	InjectionPending          InjectionState = "pending"
	InjectionInFlight         InjectionState = "in-flight"
	InjectionAccepted         InjectionState = "accepted"
	InjectionRetryableFailure InjectionState = "retryable-failure"
	InjectionRouteUnavailable InjectionState = "route-unavailable"
	InjectionTerminalFailure  InjectionState = "terminal-failure"
)

type SpoolItem struct {
	MailboxItemID    string
	AttemptID        string
	ClaimID          string
	InstanceID       string
	Generation       int64
	RecordID         string
	SpaceID          string
	RoutingKey       string
	Envelope         Envelope
	CustodyConfirmed bool
	InjectionState   InjectionState
	RuntimeReceipt   string
	FailureDetail    string
	NextRuntimeTryAt time.Time
}

type Envelope struct {
	RecordID      string
	MailboxItemID string
	AttemptID     string
	SpaceID       string
	FromPrincipal string
	SourceRun     string
	ReplyTo       string
	AddressedTo   string
	RoutingKey    string
	Body          string
}

type Route struct {
	Key           string
	RuntimeTarget string
	Enabled       bool
}
