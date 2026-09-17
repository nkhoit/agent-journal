package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	MaxContentBytes         = 64 * 1024
	MaxRelations            = 32
	MaxAttentionRecipients  = 16
	MaxPageSize             = 100
	MaxClaimBatch           = 20
	MaxLongPollSeconds      = 30
	MaxTelemetryDetailBytes = 4096
	MaxTelemetryProperties  = 32
	MaxTelemetryValueChars  = 1024
)

type Principal struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
	Disabled  bool   `json:"disabled"`
}

type Space struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

type Relation struct {
	Type     string `json:"type"`
	RecordID string `json:"record_id"`
}

const (
	RelationReplyTo      = "reply-to"
	RelationSupersedes   = "supersedes"
	RelationTombstones   = "tombstones"
	RelationRefersTo     = "refers-to"
	RelationAcknowledges = "acknowledges"
)

type RecordInput struct {
	Kind       string     `json:"kind"`
	Content    string     `json:"content"`
	RunID      string     `json:"run_id,omitempty"`
	Attention  []string   `json:"attention,omitempty"`
	RoutingKey string     `json:"routing_key,omitempty"`
	Relations  []Relation `json:"relations,omitempty"`
}

type Record struct {
	ID         string     `json:"id"`
	SpaceID    string     `json:"space_id"`
	Seq        int64      `json:"seq"`
	Author     string     `json:"author"`
	Kind       string     `json:"kind"`
	Content    string     `json:"content"`
	RunID      string     `json:"run_id,omitempty"`
	CreatedAt  string     `json:"created_at"`
	Attention  []string   `json:"attention,omitempty"`
	RoutingKey string     `json:"routing_key,omitempty"`
	Relations  []Relation `json:"relations,omitempty"`
}

type AppendResult struct {
	Record         Record `json:"record"`
	MailboxCreated int    `json:"mailbox_created"`
	Replayed       bool   `json:"replayed"`
}

type DeliveryState string

const (
	DeliveryPublished         DeliveryState = "published"
	DeliveryPending           DeliveryState = "pending"
	DeliveryClaimed           DeliveryState = "claimed"
	DeliveryHostAccepted      DeliveryState = "host-accepted"
	DeliveryRuntimeAccepted   DeliveryState = "adapter-reported-runtime-accepted"
	DeliveryRetryableFailure  DeliveryState = "adapter-reported-retryable-failure"
	DeliveryRouteUnavailable  DeliveryState = "route-unavailable"
	DeliveryTerminalFailure   DeliveryState = "adapter-reported-terminal-failure"
	DeliverySuppressedRevoked DeliveryState = "suppressed-revoked"
)

type Limits struct {
	ContentBytes         int `json:"content_bytes"`
	Relations            int `json:"relations"`
	Attention            int `json:"attention_recipients"`
	PageSize             int `json:"page_size"`
	ClaimBatch           int `json:"claim_batch"`
	LongPoll             int `json:"long_poll_seconds"`
	TelemetryDetailBytes int `json:"telemetry_detail_serialized_utf8_bytes"`
}

func DefaultLimits() Limits {
	return Limits{MaxContentBytes, MaxRelations, MaxAttentionRecipients, MaxPageSize, MaxClaimBatch, MaxLongPollSeconds, MaxTelemetryDetailBytes}
}

func (r RecordInput) Validate() error {
	if err := validateIdentifier("kind", r.Kind); err != nil {
		return err
	}
	if !utf8.ValidString(r.Content) {
		return fmt.Errorf("content: invalid UTF-8")
	}
	if r.Content == "" {
		return fmt.Errorf("content: must not be empty")
	}
	if len([]byte(r.Content)) > MaxContentBytes {
		return fmt.Errorf("content: exceeds %d bytes", MaxContentBytes)
	}
	if len(r.Relations) > MaxRelations {
		return fmt.Errorf("relations: exceeds %d items", MaxRelations)
	}
	if len(r.Attention) > MaxAttentionRecipients {
		return fmt.Errorf("attention: exceeds %d recipients", MaxAttentionRecipients)
	}
	seen := make(map[string]struct{}, len(r.Attention))
	for _, principal := range r.Attention {
		if err := validateIdentifier("attention principal", principal); err != nil {
			return err
		}
		if _, ok := seen[principal]; ok {
			return fmt.Errorf("attention: duplicate principal %q", principal)
		}
		seen[principal] = struct{}{}
	}
	replyToCount := 0
	for _, relation := range r.Relations {
		switch relation.Type {
		case RelationReplyTo, RelationSupersedes, RelationTombstones, RelationRefersTo, RelationAcknowledges:
		default:
			return fmt.Errorf("relation type %q: unsupported", relation.Type)
		}
		if err := validateIdentifier("relation record", relation.RecordID); err != nil {
			return err
		}
		if relation.Type == RelationReplyTo {
			replyToCount++
			if replyToCount > 1 {
				return fmt.Errorf("relations: at most one %q relation is allowed", RelationReplyTo)
			}
		}
	}
	if r.RoutingKey != "" {
		if err := validateIdentifier("routing_key", r.RoutingKey); err != nil {
			return err
		}
	}
	return nil
}

// ValidateTelemetryDetail applies both the structured-map limits and the
// normative serialized UTF-8 byte limit. encoding/json emits compact JSON and
// sorts map keys, which is the representation the service must measure.
func ValidateTelemetryDetail(detail map[string]string) error {
	if len(detail) > MaxTelemetryProperties {
		return fmt.Errorf("detail: exceeds %d properties", MaxTelemetryProperties)
	}
	for key, value := range detail {
		if !utf8.ValidString(key) {
			return fmt.Errorf("detail key: invalid UTF-8")
		}
		if err := validateIdentifier("detail key", key); err != nil {
			return err
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("detail value %q: invalid UTF-8", key)
		}
		if utf8.RuneCountInString(value) > MaxTelemetryValueChars {
			return fmt.Errorf("detail value %q: exceeds %d characters", key, MaxTelemetryValueChars)
		}
	}
	serialized, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("detail: serialize: %w", err)
	}
	if len(serialized) > MaxTelemetryDetailBytes {
		return fmt.Errorf("detail: serialized JSON exceeds %d UTF-8 bytes", MaxTelemetryDetailBytes)
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if value == "" || len(value) > 128 {
		return fmt.Errorf("%s: must be 1..128 characters", field)
	}
	if strings.ContainsAny(value, " /\\\t\r\n") {
		return fmt.Errorf("%s: contains forbidden whitespace or separator", field)
	}
	return nil
}

func ValidatePageSize(size int) error {
	if size < 1 || size > MaxPageSize {
		return fmt.Errorf("limit: must be between 1 and %d", MaxPageSize)
	}
	return nil
}
