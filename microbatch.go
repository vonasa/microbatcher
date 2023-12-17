package microbatcher

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type JobResult struct {
	timestamp time.Time
}

type Job any

// BatchProcessor is implemented by another library
type BatchProcessor interface {
	Process([]Job) error
}

type MicroBatchProcessor interface {
	Publish(job Job) (*JobResult, error)
	Shutdown() error
}

type microBatchImplementation struct {
	config    config
	queue     chan Job
	processor BatchProcessor
	mu        sync.RWMutex
	wgJobs    sync.WaitGroup
	active    bool
	scheduler *time.Ticker
}

type config struct {
	batchSize   int
	frequencyMs int
}

// Create an instance of the MicroBatch processor.
// batchSize - is a desired batch size
// frequencyMs - how often the batch should be sent, in milliseconds
// processor - is the implementation of the BatchProcessor interface
func Create(batchSize, frequencyMs int, processor BatchProcessor) (MicroBatchProcessor, error) {
	if processor == nil {
		return nil, errors.New("batch processor interface needs to be provided")
	}
	if batchSize <= 0 || frequencyMs <= 0 {
		return nil, errors.New("invalid configuration")
	}
	var config = config{batchSize: batchSize, frequencyMs: frequencyMs}
	microBatch := microBatchImplementation{config: config,
		queue:     make(chan Job, batchSize),
		processor: processor,
		active:    true,
		scheduler: time.NewTicker(time.Duration(frequencyMs) * time.Millisecond)}
	go microBatch.startScheduler()
	return &microBatch, nil
}

// Publish the Job to be wrapped up in a micro batch. Non Blocking operation
// Returns a job result that has the timestamp of when the job is accepted.
func (mb *microBatchImplementation) Publish(job Job) (*JobResult, error) {
	if job == nil {
		return nil, errors.New("message cannot be empty")
	}
	mb.mu.RLock()
	active := mb.active
	mb.mu.RUnlock()
	if !active {
		return nil, errors.New("the micro batcher is shut down")
	}
	mb.wgJobs.Add(1)
	go func() {
		mb.queue <- job
	}()
	return &JobResult{timestamp: time.Now()}, nil
}

// Shutdown stops accepting new messages and blocks until all already accepted messages are processed in accordance to the batch size/frequency
// error is returned if called more than once
func (mb *microBatchImplementation) Shutdown() error {
	mb.mu.RLock()
	active := mb.active
	mb.mu.RUnlock()
	if !active {
		return errors.New("the micro batcher is shut down")
	}
	mb.mu.Lock()
	mb.active = false
	mb.mu.Unlock()
	mb.wgJobs.Wait()
	mb.scheduler.Stop()
	close(mb.queue)
	return nil
}

func (mb *microBatchImplementation) startScheduler() {
	for {
		select {
		case <-mb.scheduler.C:
			mb.packageJobs()
		}
	}
}

// read jobs batch size worth.
func (mb *microBatchImplementation) packageJobs() {
	var batch []Job
out:
	for i := 0; i < mb.config.batchSize; i++ {
		select {
		case msg, ok := <-mb.queue:
			if !ok {
				break out
			}
			batch = append(batch, msg)
		default:
			break out
		}
	}
	mb.sendBatch(batch)
}

// send the batch for processing. implementation is a dependency
func (mb *microBatchImplementation) sendBatch(batch []Job) {
	if len(batch) != 0 {
		if mb.processor.Process(batch) != nil {
			fmt.Println("Error while processing batch")
		}
		for _, _ = range batch {
			mb.wgJobs.Done()
		}
	}
}
