package command

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrNotImplemented is returned by binaries that are intentionally scaffolds.
var ErrNotImplemented = errors.New("not implemented")

// RunStub makes the incomplete surface explicit instead of returning a false success.
func RunStub(w io.Writer, name string, _ []string) int {
	if _, err := fmt.Fprintf(w, "%s: not implemented (Agent Journal scaffold only)\n", name); err != nil {
		return 1
	}
	return 2
}

// Main is the shared entry point for command stubs.
func Main(name string) {
	os.Exit(RunStub(os.Stderr, name, os.Args[1:]))
}
