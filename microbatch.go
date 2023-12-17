package microbatcher

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type JobResult struct {
	time time.Time
}

type Job any

// BatchProcessor is implemented by another library
type BatchProcessor interface {
	Process([]Job) error
}

type config struct {
	batchSize   int
	frequencyMs int
}

type MicroBatchProcessor interface {
	Publish(job Job) (*JobResult, error)
	Shutdown() error
}

type microBatchImplementation struct {
	config        config
	queue         chan Job
	batchQueue    chan []Job
	processor     BatchProcessor
	mu            sync.RWMutex
	wgJobs        sync.WaitGroup
	active        bool
	schedulerQuit chan any
}

// Create an instance of the MicroBatch processor.
// batchSize - is desired batch size
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
		queue:         make(chan Job, batchSize),
		batchQueue:    make(chan []Job),
		schedulerQuit: make(chan any),
		processor:     processor,
		active:        true}
	go microBatch.startScheduler()
	go microBatch.startBatchReader()
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
	fmt.Printf("Adding a job\n")
	mb.wgJobs.Add(1)
	go func() {
		mb.queue <- job
	}()
	return &JobResult{time: time.Now()}, nil
}

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
	fmt.Printf("Waiting\n")
	mb.wgJobs.Wait()
	fmt.Printf("Finished waiting\n")
	close(mb.queue)
	close(mb.batchQueue)
	return nil
}

func (mb *microBatchImplementation) startScheduler() {
	tick := time.NewTicker(time.Duration(mb.config.frequencyMs) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			mb.packageJobs()
			fmt.Printf("Read the batch ok\n")
		}
	}
}

func (mb *microBatchImplementation) startBatchReader() {
	for {
		select {
		case batch, ok := <-mb.batchQueue:
			if !ok {
				fmt.Printf("Batch channel closed \n")
				return
			}
			fmt.Printf("Processing a batch\n")
			if len(batch) != 0 {
				if mb.processor.Process(batch) != nil {
					fmt.Println("Error while processing batch")
				}
				for _, _ = range batch {
					mb.wgJobs.Done()
				}
			}
		}
	}
}

// read jobs batch size worth. exit after frequency interval even when not enough jobs are in the batch for the full size
// still sends the batch
func (mb *microBatchImplementation) packageJobs() {
	ticker := time.NewTicker(time.Duration(mb.config.frequencyMs-20) * time.Millisecond)
	var batch []Job
	i := 0
	for {
		select {
		case <-ticker.C:
			fmt.Printf("the jobs channel has used it's time. bye\n")
			if len(batch) > 0 {
				mb.batchQueue <- batch
			}
			ticker.Stop()
			return
		case msg, ok := <-mb.queue:
			if !ok {
				fmt.Printf("the jobs channel is closed. bye\n")
				if len(batch) > 0 {
					mb.batchQueue <- batch
				}
			}
			if i < mb.config.batchSize {
				i++
				fmt.Printf("Adding a job to the batch\n")
				batch = append(batch, msg)
			}
			// got the full batch, publish it and exit
			if i == mb.config.batchSize {
				fmt.Printf("Got the batch full\n")
				mb.batchQueue <- batch
				return
			}
		}
	}
}
