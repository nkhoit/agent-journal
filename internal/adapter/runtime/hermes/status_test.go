package runtimehermes

import "testing"

func TestStatusRemainsHonest(t *testing.T) {
	adapter := Adapter{}
	if adapter.Status() != Status || adapter.Inject() != ErrRevalidationRequired {
		t.Fatalf("Hermes adapter is not an explicit revalidation gate")
	}
}
