package adaptercore

import (
	"errors"
	"fmt"
)

var ErrRouteUnavailable = errors.New("route unavailable")

// Resolve returns a configured route only. Unknown or disabled keys never
// silently fall back to a default destination.
type StaticRoutes map[string]Route

func (r StaticRoutes) Resolve(space, routingKey string) (Route, error) {
	key := routingKey
	if key == "" {
		key = "default"
	}
	route, ok := r[space+"/"+key]
	if !ok || !route.Enabled || route.RuntimeTarget == "" {
		return Route{}, fmt.Errorf("%w: %s/%s", ErrRouteUnavailable, space, key)
	}
	return route, nil
}
