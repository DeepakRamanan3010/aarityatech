package precache

import (
	"context"
	"runtime"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const maxSpin = 100

var meter = otel.GetMeterProvider().Meter("github.com/aarityatech/tickerfeed/pkg/precache")

// PreCache implements a generic pre-allocated cache.
type PreCache[T any] struct {
	entries map[int]*atomic.Pointer[cacheEntry[T]]

	// open-telemetry metrics
	spinCount metric.Int64Counter
	spinYield metric.Int64Counter
}

// cacheEntry represents an entry in the cache.
type cacheEntry[T any] struct {
	Key   int
	Value T
	IsSet bool
}

// New creates a new cache. keys is a list of keys that should be pre-allocated.
func New[T any](keys []int) *PreCache[T] {
	c := &PreCache[T]{
		entries: make(map[int]*atomic.Pointer[cacheEntry[T]], len(keys)),
	}
	c.createMeasures()

	for _, id := range keys {
		val := &atomic.Pointer[cacheEntry[T]]{}
		val.Store(&cacheEntry[T]{Key: id})
		c.entries[id] = val
	}
	return c
}

// Get returns the value for the given key. If the key is not found, then
// false is returned.
func (c *PreCache[T]) Get(keys []int) []T {
	vals := make([]T, 0, len(keys))
	for _, key := range keys {
		entryPtr, ok := c.entries[key]
		if !ok {
			continue
		}

		entry := entryPtr.Load()
		if entry.IsSet {
			vals = append(vals, entry.Value)
		}
	}
	return vals
}

// Put adds the given key-value pairs to the cache.
func (c *PreCache[T]) Put(key int, val T) {
	entryPtr, ok := c.entries[key]
	if !ok {
		return
	}

	// CAS spin update
	spins := 0
	entry := cacheEntry[T]{
		Key:   key,
		Value: val,
		IsSet: true,
	}
	for {
		old := entryPtr.Load()
		if entryPtr.CompareAndSwap(old, &entry) {
			break
		}
		spins++
		c.spinCount.Add(context.Background(), 1)

		if spins > maxSpin {
			// too many spins can mean we are in a live-lock.
			// yield the CPU and try again later.
			c.spinYield.Add(context.Background(), 1)
			runtime.Gosched()
			spins = 0
		}
	}
}

func (c *PreCache[T]) createMeasures() {
	var err error

	c.spinCount, err = meter.Int64Counter("precache.atomic.spins")
	if err != nil {
		otel.Handle(err)
	}

	c.spinYield, err = meter.Int64Counter("precache.atomic.spin_yields")
	if err != nil {
		otel.Handle(err)
	}
}
