package adaptercore

import (
	"strings"
	"testing"
)

func TestSpoolItemRecoveryFenceRequiresCustodyAndRecoverableState(t *testing.T) {
	item := SpoolItem{InjectionState: InjectionPending}
	if item.ReadyForInjection() {
		t.Fatal("uncustodied spool item marked ready for injection")
	}
	item.CustodyConfirmed = true
	if !item.ReadyForInjection() {
		t.Fatal("custody-confirmed pending item not ready for injection")
	}
	for _, state := range []InjectionState{InjectionAccepted, InjectionTerminalFailure} {
		item.InjectionState = state
		if item.ReadyForInjection() {
			t.Fatalf("%s item marked ready for reinjection", state)
		}
	}
}

func TestEventRequestValidatesSerializedDetailLimit(t *testing.T) {
	detail := map[string]string{"message": strings.Repeat("界", 1024), "padding": strings.Repeat("a", 1024)}
	request := EventRequest{Detail: detail}
	if err := request.Validate(); err == nil {
		t.Fatal("oversized telemetry detail accepted")
	}
}
