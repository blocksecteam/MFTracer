package utils

import "go.uber.org/atomic"

type AtomicErr atomic.Error

func NewError(err error) *AtomicErr {
	e := AtomicErr(*atomic.NewError(err))
	return &e
}
