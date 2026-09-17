package runtimehermes

import "errors"

const Status = "unresolved"

var ErrRevalidationRequired = errors.New("Hermes injection surface requires revalidation")

// Adapter is intentionally unavailable until a supported persistent-session
// injection surface and acceptance receipt are revalidated.
type Adapter struct{}

func (Adapter) Status() string { return Status }
func (Adapter) Inject() error  { return ErrRevalidationRequired }
