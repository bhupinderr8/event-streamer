package workerpool

import (
	"context"
	"sync"

	"github.com/bhupinder121199/event-streamer/internal/broker"
	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"google.golang.org/protobuf/proto"
)

// WorkerPool provides a non-blocking buffered pipeline to ingest and stream events.
type WorkerPool struct {
	queue    chan *eventv1.IngestRequest
	producer broker.Producer
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// New creates and starts a new non-blocking worker pool.
// New creates and starts a high-capacity non-blocking worker pool.
func New(producer broker.Producer, numWorkers int, bufferCapacity int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = 32
	}
	if bufferCapacity <= 0 {
		bufferCapacity = 65536
		bufferCapacity = 131072
	}

	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		queue:    make(chan *eventv1.IngestRequest, bufferCapacity),
		producer: producer,
		ctx:      ctx,
		cancel:   cancel,
	}

	for i := 0; i < numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	return wp
}

// Submit non-blockingly places an ingestion request into the worker buffer.
// Returns false if the queue is full (backpressure shedding).
func (wp *WorkerPool) Submit(req *eventv1.IngestRequest) bool {
	select {
	case wp.queue <- req:
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()

	for {
		select {
		case req, ok := <-wp.queue:
			if !ok {
				return
			}
			data, err := proto.Marshal(req)
			if err == nil {
				_ = wp.producer.Publish(wp.ctx, "", req.GetEventId(), data)
			}
		case <-wp.ctx.Done():
			// Drain remaining events in buffer
			for {
				select {
				case req, ok := <-wp.queue:
					if !ok {
						return
					}
					data, err := proto.Marshal(req)
					if err == nil {
						_ = wp.producer.Publish(context.Background(), "", req.GetEventId(), data)
					}
				default:
					return
				}
			}
		}
	}
}

// Stop terminates the worker pool and drains in-flight events.
// Stop safely drains and terminates the worker pool.
func (wp *WorkerPool) Stop() {
	wp.cancel()
	close(wp.queue)
	wp.wg.Wait()
}
