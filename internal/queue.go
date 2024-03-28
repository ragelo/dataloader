package internal

import (
	"context"
	"math"
	"sync"
	"time"
)

type QueueObject[K string] struct {
	key K
	ch  chan bool
}

type Queue[K string] struct {
	BatchChan chan *[]K

	ch             chan K
	timeoutCh      chan bool
	maxBatchSize   int32
	maxBatchTimeMs int32
	keys           []*QueueObject[K]
	keysMap        map[K]bool
	mut            sync.RWMutex
}

func NewQueue[K string](maxBatchSize int32, maxBatchTimeMs int32) *Queue[K] {
	return &Queue[K]{
		ch:             make(chan K),
		timeoutCh:      make(chan bool),
		maxBatchSize:   maxBatchSize,
		maxBatchTimeMs: maxBatchTimeMs,
		keys:           make([]*QueueObject[K], 0),
		keysMap:        make(map[K]bool),
		BatchChan:      make(chan *[]K),
	}
}

func (q *Queue[K]) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case key := <-q.ch:
				found := false
				if _, ok := q.keysMap[key]; ok {
					found = true
				}

				// if key not on keys, add for processing
				if !found {
					ch := make(chan bool)

					q.mut.Lock()
					q.keys = append(q.keys, &QueueObject[K]{key: key, ch: ch})
					q.keysMap[key] = true
					q.mut.Unlock()

					go func() {
						for {
							select {
							case <-ch:
								return
							case <-time.After(time.Duration(q.maxBatchTimeMs) * time.Millisecond):
								q.dispatch()
							}
						}
					}()
				}

				if int32(len(q.keys)) >= q.maxBatchSize {
					q.dispatch()
				}
			}
		}
	}()
}

func (q *Queue[K]) Append(key K) {
	go func() {
		q.ch <- key
	}()
}

func (q *Queue[K]) dispatch() {
	if len(q.keys) == 0 {
		return
	}

	q.mut.Lock()
	batchSize := int(math.Min(float64(len(q.keys)), float64(q.maxBatchSize)))
	keys := make([]K, batchSize)
	for i, item := range q.keys[:batchSize] {
		keys[i] = item.key
		close(item.ch)
	}

	q.BatchChan <- &keys

	if len(q.keys) > batchSize {
		q.keys = q.keys[batchSize:]
	} else {
		q.keys = make([]*QueueObject[K], 0)
	}
	for _, key := range keys {
		delete(q.keysMap, key)
	}
	q.mut.Unlock()
}
