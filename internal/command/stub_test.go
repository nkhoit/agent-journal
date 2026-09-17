package command

import (
	"bytes"
	"testing"
)

func TestRunStubIsExplicitAndNonZero(t *testing.T) {
	var out bytes.Buffer
	if got := RunStub(&out, "journald", nil); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
	want := "journald: not implemented (Agent Journal scaffold only)\n"
	if out.String() != want {
		t.Fatalf("output = %q, want %q", out.String(), want)
	}
}
