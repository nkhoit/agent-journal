package adaptercore

import (
	"context"
	"errors"
	"testing"
)

func TestStaticRoutesDoesNotFallbackOnUnknownKey(t *testing.T) {
	routes := StaticRoutes{"space/default": {Key: "default", RuntimeTarget: "local-default", Enabled: true}}
	if _, err := routes.Resolve("space", "typo"); !errors.Is(err, ErrRouteUnavailable) {
		t.Fatalf("unknown route error = %v, want ErrRouteUnavailable", err)
	}
}

func TestStaticRoutesUsesDefaultOnlyWhenKeyMissing(t *testing.T) {
	routes := StaticRoutes{"space/default": {Key: "default", RuntimeTarget: "local-default", Enabled: true}}
	got, err := routes.Resolve("space", "")
	if err != nil || got.RuntimeTarget != "local-default" {
		t.Fatalf("default route = %#v, err=%v", got, err)
	}
}

type recordingRuntime struct {
	route    Route
	envelope Envelope
}

func (r *recordingRuntime) Inject(_ context.Context, route Route, envelope Envelope) (string, error) {
	r.route = route
	r.envelope = envelope
	return "runtime-receipt", nil
}

func TestInjectResolvedRoutePassesResolvedRouteToRuntime(t *testing.T) {
	routes := StaticRoutes{"space/project": {Key: "project", RuntimeTarget: "local-target", Enabled: true}}
	runtime := new(recordingRuntime)
	item := SpoolItem{
		Envelope:         Envelope{SpaceID: "space", RoutingKey: "project", RecordID: "record-1"},
		CustodyConfirmed: true,
		InjectionState:   InjectionPending,
	}
	receipt, err := InjectResolvedRoute(context.Background(), routes, runtime, item)
	if err != nil {
		t.Fatalf("inject: %v", err)
	}
	if receipt != "runtime-receipt" {
		t.Fatalf("receipt = %q, want runtime-receipt", receipt)
	}
	if runtime.route.RuntimeTarget != "local-target" {
		t.Fatalf("runtime route = %#v, want resolved local target", runtime.route)
	}
	if runtime.envelope != item.Envelope {
		t.Fatalf("runtime envelope = %#v, want %#v", runtime.envelope, item.Envelope)
	}
}

func TestInjectResolvedRouteDoesNotCallRuntimeForUnavailableRoute(t *testing.T) {
	runtime := new(recordingRuntime)
	item := SpoolItem{
		Envelope:         Envelope{SpaceID: "space", RoutingKey: "missing"},
		CustodyConfirmed: true,
		InjectionState:   InjectionPending,
	}
	_, err := InjectResolvedRoute(context.Background(), StaticRoutes{}, runtime, item)
	if !errors.Is(err, ErrRouteUnavailable) {
		t.Fatalf("error = %v, want ErrRouteUnavailable", err)
	}
	if runtime.route != (Route{}) {
		t.Fatalf("runtime was called for unavailable route: %#v", runtime.route)
	}
}

func TestInjectResolvedRouteRequiresHostCustody(t *testing.T) {
	runtime := new(recordingRuntime)
	item := SpoolItem{Envelope: Envelope{SpaceID: "space", RoutingKey: "project"}, InjectionState: InjectionPending}
	_, err := InjectResolvedRoute(context.Background(), StaticRoutes{"space/project": {Key: "project", RuntimeTarget: "local-target", Enabled: true}}, runtime, item)
	if !errors.Is(err, ErrCustodyNotConfirmed) {
		t.Fatalf("error = %v, want ErrCustodyNotConfirmed", err)
	}
	if runtime.route != (Route{}) {
		t.Fatalf("runtime was called before custody confirmation: %#v", runtime.route)
	}
}
