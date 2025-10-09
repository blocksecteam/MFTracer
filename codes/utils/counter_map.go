package utils

import (
	"sync"
)

type CounterMap[K comparable] struct {
	sync.Mutex
	m map[K]uint64
}

func NewCounterMap[K comparable]() *CounterMap[K] {
	return &CounterMap[K]{m: make(map[K]uint64)}
}

func (m *CounterMap[K]) Map() map[K]uint64 {
	return m.m
}

func (m *CounterMap[K]) Add(k K, v uint64) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.m[k]; !ok {
		m.m[k] = v
	} else {
		m.m[k] += v
	}
}
