package dataloader

import (
	"context"
	"math"
	"sync"
	"time"
)

type queueObject[K string] struct {
	key K
	ch  chan bool
}

type queueManager[K string] interface {
	// Append adds a key to the queue.
	Append(key K)
	// GetBatchChan returns a channel that will receive a batch of keys when the batch is ready to be processed.
	GetBatchChan() chan *[]K
}

type defaultQueueManager[K string] struct {
	batchChan chan *[]K

	ch             chan K
	maxBatchSize   int32
	maxBatchTimeMs int32
	keys           []*queueObject[K]
	keysMap        map[K]bool
	mut            sync.RWMutex
}

func (q *defaultQueueManager[K]) Append(key K) {
	go func() {
		q.ch <- key
	}()
}

func (q *defaultQueueManager[K]) GetBatchChan() chan *[]K {
	return q.batchChan
}

func newDefaultQueueManager[K string](ctx context.Context, maxBatchSize int32, maxBatchTimeMs int32) *defaultQueueManager[K] {
	qm := &defaultQueueManager[K]{
		ch:             make(chan K),
		maxBatchSize:   maxBatchSize,
		maxBatchTimeMs: maxBatchTimeMs,
		keys:           make([]*queueObject[K], 0),
		keysMap:        make(map[K]bool),
		batchChan:      make(chan *[]K),
	}
	qm.start(ctx)
	return qm
}

func (q *defaultQueueManager[K]) start(ctx context.Context) {
	// Start the queue processing: listen for new keys and dispatch them
	go func() {
		for {
			select {
			case <-ctx.Done():
				q.mut.Lock()
				close(q.ch)
				close(q.batchChan)
				q.mut.Unlock()
				return
			case key := <-q.ch:
				q.handleNewKey(key)
			}
		}
	}()
}

func (q *defaultQueueManager[K]) handleNewKey(key K) {
	q.mut.RLock()
	found := false
	if _, ok := q.keysMap[key]; ok {
		found = true
	}
	q.mut.RUnlock()

	// if key not on keys, add for processing
	if !found {
		ch := make(chan bool)

		q.mut.Lock()
		q.keys = append(q.keys, &queueObject[K]{key: key, ch: ch})
		q.keysMap[key] = true
		q.mut.Unlock()

		go func() {
			q.mut.RLock()
			fireAt := time.Duration(q.maxBatchTimeMs) * time.Millisecond
			q.mut.RUnlock()
			for {
				select {
				case <-ch:
					return
				case <-time.After(fireAt):
					// Dispatch the batch if the max wait time is reached
					q.dispatch()
				}
			}
		}()
	}
	q.mut.RLock()
	shouldTrigger := int32(len(q.keys)) >= q.maxBatchSize
	q.mut.RUnlock()

	if shouldTrigger {
		// Dispatch the batch if the max batch size is reached
		q.dispatch()
	}
}

func (q *defaultQueueManager[K]) dispatch() {
	q.mut.RLock()
	if len(q.keys) == 0 {
		q.mut.RUnlock()
		return
	}

	q.mut.RUnlock()
	q.mut.Lock()
	batchSize := int(math.Min(float64(len(q.keys)), float64(q.maxBatchSize)))
	keys := make([]K, batchSize)
	for i, item := range q.keys[:batchSize] {
		keys[i] = item.key
		close(item.ch)
	}

	q.batchChan <- &keys

	if len(q.keys) > batchSize {
		q.keys = q.keys[batchSize:]
	} else {
		q.keys = make([]*queueObject[K], 0)
	}
	for _, key := range keys {
		delete(q.keysMap, key)
	}
	q.mut.Unlock()
}
