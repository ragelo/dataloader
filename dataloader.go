package dataloader

import (
	"context"
	"sync"
)

type BatchLoader[K string | int, T any] interface {
	// Batch function that is called with a list of keys. This function should return a map of values for each passed key.
	// Should be implemented by the user.
	BatchLoad(ctx context.Context, keys *[]K) (map[K]*T, error)
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
	batchLoader  BatchLoader[K, T]
	queueManager queueManager[K]

	cache map[K]*cacheEntity[T]
	lock  sync.RWMutex
	ctx   context.Context
}

func NewDataLoader[K string, T any](ctx context.Context, batchLoader BatchLoader[K, T], maxBatchSize int32, maxBatchTimeMs int32) *DataLoader[K, T] {
	loader := &DataLoader[K, T]{
		batchLoader:  batchLoader,
		cache:        make(map[K]*cacheEntity[T]),
		queueManager: newDefaultQueueManager[K](ctx, maxBatchSize, maxBatchTimeMs),
		lock:         sync.RWMutex{},
		ctx:          ctx,
	}
	loader.start(ctx)
	return loader
}

// Load retrieves a value for a given key. If the value is not already cached, the loader will call the batch function with a list of keys.
func (d *DataLoader[K, T]) Load(key K) (*T, error) {
	// Load retrieves a value for a given key.
	d.lock.RLock()
	if hit, ok := d.cache[key]; ok && hit.resolved {
		if hit.err != nil {
			d.lock.RUnlock()
			return nil, hit.err
		}
		d.lock.RUnlock()
		return hit.value, nil
	}
	d.lock.RUnlock()

	// Lock the cache to ensire only one goroutine is processing given key.
	hit := d.loadKey(key)

	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	case <-hit.ch:
		return hit.value, hit.err
	}
}

// LoadMany retrieves multiple values for a given list of keys. For each key that is not already cached, a single loader call will be scheduled.
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
		d.queueManager.Append(key)
	}
	d.lock.Unlock()
	return hit
}

func (d *DataLoader[K, T]) start(ctx context.Context) {
	// Start main processing loop.
	go func() {
		batchChunkChannel := d.queueManager.GetBatchChan()
		for {
			select {
			case <-ctx.Done():
				return
			case keys := <-batchChunkChannel:
				// Process each new chunk of keys evicted from the queue.
				d.processChunk(keys)
			}
		}
	}()
}

func (d *DataLoader[K, T]) processChunk(chunk *[]K) {
	// Call the batch function with a list of keys. Then resolve the values for each key.
	values, err := d.batchLoader.BatchLoad(d.ctx, chunk)
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
