package domain

import (
	"strings"
	"testing"
)

func TestRecordInputValidateLimitsAndDuplicates(t *testing.T) {
	valid := RecordInput{Kind: "message", Content: "safe", Attention: []string{"beta"}, Relations: []Relation{{Type: "reply-to", RecordID: "record-1"}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid input: %v", err)
	}
	tooLarge := valid
	tooLarge.Content = strings.Repeat("x", MaxContentBytes+1)
	if err := tooLarge.Validate(); err == nil {
		t.Fatal("oversized content accepted")
	}
	duplicate := valid
	duplicate.Attention = []string{"beta", "beta"}
	if err := duplicate.Validate(); err == nil {
		t.Fatal("duplicate attention accepted")
	}
}

func TestRecordInputCountsUTF8Bytes(t *testing.T) {
	exact := RecordInput{
		Kind:    "message",
		Content: strings.Repeat("界", MaxContentBytes/3) + strings.Repeat("a", MaxContentBytes%3),
	}
	if err := exact.Validate(); err != nil {
		t.Fatalf("exact UTF-8 byte limit rejected: %v", err)
	}
	over := exact
	over.Content += "a"
	if err := over.Validate(); err == nil {
		t.Fatal("content over UTF-8 byte limit accepted")
	}
}

func TestRecordInputRejectsEmptyContent(t *testing.T) {
	input := RecordInput{Kind: "message"}
	if err := input.Validate(); err == nil {
		t.Fatal("empty content accepted")
	}
}

func TestRecordInputAcceptsExactRelationVocabulary(t *testing.T) {
	for _, relationType := range []string{RelationReplyTo, RelationSupersedes, RelationTombstones, RelationRefersTo, RelationAcknowledges} {
		input := RecordInput{Kind: "message", Content: "x", Relations: []Relation{{Type: relationType, RecordID: "record-1"}}}
		if err := input.Validate(); err != nil {
			t.Errorf("relation type %q rejected: %v", relationType, err)
		}
	}
}

func TestRecordInputRejectsMultipleReplyToRelations(t *testing.T) {
	input := RecordInput{
		Kind:    "message",
		Content: "x",
		Relations: []Relation{
			{Type: RelationReplyTo, RecordID: "record-1"},
			{Type: RelationReplyTo, RecordID: "record-2"},
		},
	}
	if err := input.Validate(); err == nil {
		t.Fatal("multiple reply-to relations accepted")
	}
}

func TestRecordInputRejectsUnsupportedRelation(t *testing.T) {
	input := RecordInput{Kind: "message", Content: "x", Relations: []Relation{{Type: "executes", RecordID: "record-1"}}}
	if err := input.Validate(); err == nil {
		t.Fatal("unsupported relation accepted")
	}
}

func TestValidateTelemetryDetailUsesSerializedUTF8ByteLimit(t *testing.T) {
	base := strings.Repeat("界", MaxTelemetryValueChars)
	var largest map[string]string
	var oversized map[string]string
	for paddingLength := 0; ; paddingLength++ {
		candidate := map[string]string{
			"base":    base,
			"padding": strings.Repeat("a", paddingLength),
		}
		if err := ValidateTelemetryDetail(candidate); err != nil {
			oversized = candidate
			break
		}
		largest = candidate
	}
	if largest == nil || oversized == nil {
		t.Fatal("did not find adjacent valid and oversized telemetry details")
	}
	if err := ValidateTelemetryDetail(largest); err != nil {
		t.Fatalf("largest valid detail rejected: %v", err)
	}
	if err := ValidateTelemetryDetail(oversized); err == nil || !strings.Contains(err.Error(), "4096 UTF-8 bytes") {
		t.Fatalf("oversized detail error = %v, want serialized byte-limit error", err)
	}
}

func TestValidatePageSize(t *testing.T) {
	for _, size := range []int{0, MaxPageSize + 1} {
		if err := ValidatePageSize(size); err == nil {
			t.Errorf("size %d accepted", size)
		}
	}
	if err := ValidatePageSize(MaxPageSize); err != nil {
		t.Fatalf("maximum size rejected: %v", err)
	}
}
