package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/bhupinder121199/event-streamer/internal/broker"
	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// stringToBytes performs an unsafe zero-allocation conversion from string to byte slice.
func stringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// WorkerPool provides a sharded, non-blocking buffered pipeline to ingest and stream events.
type WorkerPool struct {
	queues        []chan *eventv1.IngestRequest
	producer      broker.Producer
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	stopped       atomic.Bool
	publishErrors atomic.Uint64
	roundRobin    atomic.Uint64
}

const workerBatchSize = 128

// New creates and starts a high-capacity non-blocking sharded worker pool.
func New(producer broker.Producer, numWorkers int, bufferCapacity int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = 32
	}
	if bufferCapacity <= 0 {
		bufferCapacity = 131072
	}

	shardCap := bufferCapacity / numWorkers
	if shardCap < 1024 {
		shardCap = 1024
	}

	ctx, cancel := context.WithCancel(context.Background())
	queues := make([]chan *eventv1.IngestRequest, numWorkers)
	for i := 0; i < numWorkers; i++ {
		queues[i] = make(chan *eventv1.IngestRequest, shardCap)
	}

	wp := &WorkerPool{
		queues:   queues,
		producer: producer,
		ctx:      ctx,
		cancel:   cancel,
	}

	for i := 0; i < numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

// Submit non-blockingly places an ingestion request into the worker pool.
// Returns false if the pool is stopped or the queue shards are saturated (backpressure shedding).
func (wp *WorkerPool) Submit(req *eventv1.IngestRequest) bool {
	if wp.stopped.Load() {
		return false
	}
	n := uint64(len(wp.queues))
	start := wp.roundRobin.Add(1) % n

	// Try the assigned shard first, then 1 fallback shard before shedding
	for i := uint64(0); i < 2; i++ {
		shard := (start + i) % n
		select {
		case wp.queues[shard] <- req:
			return true
		default:
		}
	}
	return false
}

// SubmitBatch non-blockingly places a slice of ingestion requests into a worker buffer shard.
// Returns the number of successfully enqueued events and true if the entire batch was accepted.
func (wp *WorkerPool) SubmitBatch(events []*eventv1.IngestRequest) (int, bool) {
	if wp.stopped.Load() {
		return 0, false
	}
	n := uint64(len(wp.queues))
	start := wp.roundRobin.Add(1) % n
	q := wp.queues[start]

	accepted := 0
	for _, req := range events {
		select {
		case q <- req:
			accepted++
		default:
			return accepted, false
		}
	}
	return accepted, true
}

// QueueDepth returns the total number of events currently buffered across all shards.
func (wp *WorkerPool) QueueDepth() int {
	total := 0
	for _, q := range wp.queues {
		total += len(q)
	}
	return total
}

func (wp *WorkerPool) worker(shardID int) {
	defer wp.wg.Done()
	q := wp.queues[shardID]
	batch := make([]kafka.Message, 0, workerBatchSize)

	for {
		select {
		case req, ok := <-q:
			if !ok {
				return
			}
			batch = batch[:0]
			batchTime := time.Now()

			if data, err := proto.Marshal(req); err == nil {
				batch = append(batch, kafka.Message{
					Key:   stringToBytes(req.GetEventId()),
					Value: data,
					Time:  batchTime,
				})
			}

			// Drain up to workerBatchSize items without blocking
		drain:
			for len(batch) < workerBatchSize {
				select {
				case nextReq, ok := <-q:
					if !ok {
						break drain
					}
					if data, err := proto.Marshal(nextReq); err == nil {
						batch = append(batch, kafka.Message{
							Key:   stringToBytes(nextReq.GetEventId()),
							Value: data,
							Time:  batchTime,
						})
					}
				default:
					break drain
				}
			}

			if len(batch) > 0 {
				if err := wp.producer.PublishBatch(wp.ctx, batch); err != nil {
					wp.publishErrors.Add(1)
				}
			}

		case <-wp.ctx.Done():
			// Drain remaining events in buffer
			for {
				batch = batch[:0]
				batchTime := time.Now()
			drainRemaining:
				for len(batch) < workerBatchSize {
					select {
					case req, ok := <-q:
						if !ok {
							break drainRemaining
						}
						if data, err := proto.Marshal(req); err == nil {
							batch = append(batch, kafka.Message{
								Key:   stringToBytes(req.GetEventId()),
								Value: data,
								Time:  batchTime,
							})
						}
					default:
						break drainRemaining
					}
				}
				if len(batch) == 0 {
					return
				}
				if err := wp.producer.PublishBatch(context.Background(), batch); err != nil {
					wp.publishErrors.Add(1)
				}
			}
		}
	}
}

// PublishErrors returns the count of failed batch publish operations.
func (wp *WorkerPool) PublishErrors() uint64 {
	return wp.publishErrors.Load()
}

// Stop safely drains and terminates the worker pool.
func (wp *WorkerPool) Stop() {
	wp.stopped.Store(true)
	wp.cancel()
	for _, q := range wp.queues {
		close(q)
	}
	wp.wg.Wait()
}
