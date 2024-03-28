package dataloader

import (
	"context"
	"sync"

	dli "github.com/ragelo/dataloader/internal"
)

type IDataLoaderConfig[K string | int, T any] interface {
	// Batch function that is called with a list of keys. This function should return a list of values in the same order as the keys.
	// Shoulb be implemented by the user.
	BatchLoad(ctx context.Context, keys *[]K) (map[K]*T, error)
}

// IDataLoader abstract class that batches multiple requests into a single database request.
// IDataLoader is a generic utility to be used in a GraphQL server to batch and cache requests to a backend database.
type IDataLoader[K string, T any] interface {
	// Load retrieves a value for a given key. If the value is not already cached, the loader will call the batch function with a list of keys.
	Load(key K) (*T, error)
	// LoadMany retrieves multiple values for a given list of keys. For each key that is not already cached, a single loader call will be scheduled.
	LoadMany(keys []K) ([]*T, error)
}

type ResolvedValue[T any] struct {
	value *T
	err   error
}

type CacheEntity[T any] struct {
	value    *T
	resolved bool
	ch       chan ResolvedValue[T]
	err      error
}

type DataLoader[K string, T any] struct {
	IDataLoader[K, T]

	config IDataLoaderConfig[K, T]
	cache  map[K]*CacheEntity[T]

	queue *dli.Queue[K]

	lock sync.RWMutex
	ctx  context.Context
}

func NewDataLoader[K string, T any](ctx context.Context, config IDataLoaderConfig[K, T], maxBatchSize int32, maxBatchTimeMs int32) *DataLoader[K, T] {
	loader := &DataLoader[K, T]{
		config: config,
		cache:  make(map[K]*CacheEntity[T]),
		queue:  dli.NewQueue[K](maxBatchSize, maxBatchTimeMs),
		lock:   sync.RWMutex{},
		ctx:    ctx,
	}
	loader.start(ctx)
	return loader
}

func (d *DataLoader[K, T]) start(ctx context.Context) {
	d.queue.Start(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case keys := <-d.queue.BatchChan:
				d.processChunk(keys)
			}
		}
	}()
}

func (d *DataLoader[K, T]) Load(key K) (*T, error) {
	// Use batchLoad to load the value for the key.
	// If the value is not already cached, the loader will call the batch function with a list of keys.
	// If the value is already cached, the value will be returned.
	// Batch function should be called every 30 milliseconds or when the batch size reaches 1000.

	if hit, ok := d.cache[key]; ok && hit.resolved {
		if hit.err != nil {
			return nil, hit.err
		}
		return hit.value, nil
	}

	// Lock the cache to ensire only one goroutine is processing given key.
	hit := d.loadKey(key)

	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case <-hit.ch:
		return hit.value, hit.err
	}
}

func (d *DataLoader[K, T]) loadKey(key K) *CacheEntity[T] {
	d.lock.Lock()
	hit, ok := d.cache[key]
	if !ok {
		// If the key is not being processed, create a new channel and submit the key to the processing queue.
		hit = &CacheEntity[T]{resolved: false, ch: make(chan ResolvedValue[T])}
		d.cache[key] = hit
		d.queue.Append(key)
	}
	d.lock.Unlock()
	return hit
}

func (d *DataLoader[K, T]) LoadMany(keys *[]K) ([]*T, error) {
	hits := []*CacheEntity[T]{}
	for _, key := range *keys {
		hits = append(hits, d.loadKey(key))
	}

	values := []*T{}
	for _, hit := range hits {
		select {
		case <-d.ctx.Done():
			return nil, d.ctx.Err()
		case <-hit.ch:
			if hit.err != nil {
				return nil, hit.err
			}
			values = append(values, hit.value)
		}
	}
	return values, nil
}

func (d *DataLoader[K, T]) processChunk(chunk *[]K) {
	values, err := d.config.BatchLoad(d.ctx, chunk)
	for k, v := range values {
		d.resolveKey(k, v, err)
	}
}

func (d *DataLoader[K, T]) resolveKey(k K, v *T, err error) {
	d.lock.Lock()
	c := d.cache[k]
	if c == nil {
		c = &CacheEntity[T]{}
		d.cache[k] = c
	}

	c.resolved = true

	if err != nil {
		c.err = err
	} else {
		c.value = v
	}
	d.lock.Unlock()
	close(c.ch)
}
