package runtimemuse

import "testing"

func TestStatusRemainsHonest(t *testing.T) {
	adapter := Adapter{}
	if adapter.Status() != Status || adapter.Inject() != ErrRevalidationRequired {
		t.Fatalf("Muse adapter is not an explicit revalidation gate")
	}
}
