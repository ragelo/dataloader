package dataloader

import (
	"context"
	"sync"

	dli "github.com/ragelo/dataloader/internal"
)

type IDataLoaderConfig[K string | int, T any] interface {
	// Batch function that is called with a list of keys. This function should return a map of values for each passed key.
	// Should be implemented by the user.
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

type resolvedValue[T any] struct {
	value *T
	err   error
}

type cacheEntity[T any] struct {
	value    *T
	resolved bool
	ch       chan resolvedValue[T]
	err      error
}

type DataLoader[K string, T any] struct {
	IDataLoader[K, T]

	config IDataLoaderConfig[K, T]
	cache  map[K]*cacheEntity[T]

	queue *dli.Queue[K]

	lock sync.RWMutex
	ctx  context.Context
}

func NewDataLoader[K string, T any](ctx context.Context, config IDataLoaderConfig[K, T], maxBatchSize int32, maxBatchTimeMs int32) *DataLoader[K, T] {
	loader := &DataLoader[K, T]{
		config: config,
		cache:  make(map[K]*cacheEntity[T]),
		queue:  dli.NewQueue[K](maxBatchSize, maxBatchTimeMs),
		lock:   sync.RWMutex{},
		ctx:    ctx,
	}
	loader.start(ctx)
	return loader
}

func (d *DataLoader[K, T]) Load(key K) (*T, error) {
	// Load retrieves a value for a given key.
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

func (d *DataLoader[K, T]) LoadMany(keys *[]K) ([]*T, error) {
	// LoadMany retrieves multiple values for a given list of keys.
	hits := []*cacheEntity[T]{}
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

func (d *DataLoader[K, T]) loadKey(key K) *cacheEntity[T] {
	// Load the value for the key. If the key is not being processed, create a new channel and submit the key to the processing queue.
	d.lock.Lock()
	hit, ok := d.cache[key]
	if !ok {
		// If the key is not being processed, create a new channel and submit the key to the processing queue.
		hit = &cacheEntity[T]{resolved: false, ch: make(chan resolvedValue[T])}
		d.cache[key] = hit
		d.queue.Append(key)
	}
	d.lock.Unlock()
	return hit
}

func (d *DataLoader[K, T]) start(ctx context.Context) {
	// Start the queue processing loop.
	d.queue.Start(ctx)

	// Start main processing loop.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case keys := <-d.queue.BatchChan:
				// Process each new chunk of keys evicted from the queue.
				d.processChunk(keys)
			}
		}
	}()
}

func (d *DataLoader[K, T]) processChunk(chunk *[]K) {
	// Call the batch function with a list of keys. Then resolve the values for each key.
	values, err := d.config.BatchLoad(d.ctx, chunk)
	for k, v := range values {
		d.resolveKey(k, v, err)
	}
}

func (d *DataLoader[K, T]) resolveKey(k K, v *T, err error) {
	// Resolve the value for the key. Writes the value to the cache and closes the channel.
	d.lock.Lock()
	c := d.cache[k]
	if c == nil {
		c = &cacheEntity[T]{}
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
