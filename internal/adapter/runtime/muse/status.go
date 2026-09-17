package runtimemuse

import "errors"

const Status = "unresolved"

var ErrRevalidationRequired = errors.New("Muse injection surface requires revalidation")

type Adapter struct{}

func (Adapter) Status() string { return Status }
func (Adapter) Inject() error  { return ErrRevalidationRequired }
