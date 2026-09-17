package sqlite

import "testing"

func TestConnectionPragmasCaptureDurabilityInvariants(t *testing.T) {
	got := ConnectionPragmas()
	want := []string{"PRAGMA foreign_keys = ON", "PRAGMA journal_mode = WAL", "PRAGMA synchronous = FULL", "PRAGMA busy_timeout = 5000"}
	if len(got) != len(want) {
		t.Fatalf("pragma count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pragma[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
