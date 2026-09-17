package adaptercore

import (
	"context"
	"errors"
)

var ErrCustodyNotConfirmed = errors.New("host custody not confirmed")

// InjectResolvedRoute resolves the private local route before handing the
// authenticated envelope to the runtime. Runtime implementations must never
// resolve routes themselves or receive a portable routing key in place of the
// resolved target.
func InjectResolvedRoute(ctx context.Context, resolver RouteResolver, runtime Runtime, item SpoolItem) (string, error) {
	if !item.ReadyForInjection() {
		return "", ErrCustodyNotConfirmed
	}
	route, err := resolver.Resolve(item.Envelope.SpaceID, item.Envelope.RoutingKey)
	if err != nil {
		return "", err
	}
	return runtime.Inject(ctx, route, item.Envelope)
}

// ReadyForInjection is the local custody fence. Accepted and terminal rows are
// retained for deduplication but cannot be injected again by normal recovery.
func (item SpoolItem) ReadyForInjection() bool {
	if !item.CustodyConfirmed {
		return false
	}
	switch item.InjectionState {
	case InjectionPending, InjectionInFlight, InjectionRetryableFailure, InjectionRouteUnavailable:
		return true
	default:
		return false
	}
}
