package microbatcher

import (
	"fmt"
	"testing"
	"time"
)

type mockBatchProcessor struct {
	process func(batch []Job) error
}

func (bp *mockBatchProcessor) Process(batch []Job) error {
	return bp.process(batch)
}

func TestCreate(t *testing.T) {
	t.Parallel()
	mock := &mockBatchProcessor{process: func(batch []Job) error { return nil }}
	tests := []struct {
		name        string
		batchSize   int
		frequencyMs int
		processor   BatchProcessor
	}{
		{name: "BatchProcessor nil", batchSize: 1, frequencyMs: 1, processor: nil},
		{name: "Batch size 0", batchSize: 0, frequencyMs: 1, processor: mock},
		{name: "Batch size negative", batchSize: -4, frequencyMs: 1, processor: mock},
		{name: "Frequency 0", batchSize: 5, frequencyMs: 0, processor: mock},
		{name: "Frequency negative", batchSize: 5, frequencyMs: -5, processor: mock},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Create(tt.batchSize, tt.frequencyMs, tt.processor)
			if err == nil {
				t.Errorf("Create() is expected to return an error")
				return
			}
		})
	}
}

func TestMicroBatchImplementation_Publish(t *testing.T) {
	var batchTimes []time.Time
	batchSize := 2
	frequencyMs := 300
	expectedBatches := 2
	mock := &mockBatchProcessor{process: func(batch []Job) error {
		fmt.Printf("Called batch processing with %v jobs, time is %s\n", len(batch), time.Now())
		if len(batch) != batchSize {
			t.Errorf("Batch size is excepted to be %d but instead is %d", batchSize, batch)
		}
		batchTimes = append(batchTimes, time.Now())
		return nil
	}}
	t.Run("Should create correct number of batches when all batches are complete", func(t *testing.T) {
		mb, err := Create(batchSize, frequencyMs, mock)
		if err != nil {
			t.Errorf("Create() is failed")
			return
		}
		start := time.Now()
		for i := 0; i < expectedBatches*batchSize; i++ {
			jr, err := mb.Publish(struct{}{})
			if err != nil || jr == nil {
				t.Errorf("Publish() is failed")
				return
			}
		}
		time.Sleep(time.Duration(frequencyMs*(expectedBatches)+frequencyMs*2) * time.Millisecond)
		err = mb.Shutdown()
		if err != nil {
			t.Errorf("Shutdown failed")
		}
		_, err = mb.Publish(struct{}{})
		if err == nil {
			t.Errorf("Publish() after shutdown should result in an error")
			return
		}
		err = mb.Shutdown()
		if err == nil {
			t.Errorf("Second shutdown should result in an error")
		}
		if len(batchTimes) != expectedBatches {
			t.Errorf("Expected %d batches to be processed but got %d", expectedBatches, len(batchTimes))
			return
		}
		for batchIdx, timestamp := range batchTimes {
			expectedTime := start.Add(time.Duration((batchIdx+1)*frequencyMs) * time.Millisecond)
			if !closeEnough(timestamp, expectedTime) {
				t.Errorf("Batch number %d wasn't processed when expected. The expected time is %s, the actual time is %s", batchIdx+1, expectedTime, timestamp)
			}
		}
	})
}

func TestMicroBatchImplementation_Publish_ReadAllMessagesAfterShutdown(t *testing.T) {
	batchSize := 6
	frequencyMs := 200
	expectedJobs := 100
	processedJobs := 0
	mock := &mockBatchProcessor{process: func(batch []Job) error {
		fmt.Printf("Called batch processing with %v jobs, time is %s\n", len(batch), time.Now())
		processedJobs += len(batch)
		return nil
	}}
	t.Run("Should finish reading all messages after the shutdown", func(t *testing.T) {
		mb, err := Create(batchSize, frequencyMs, mock)
		if err != nil {
			t.Errorf("Create() is failed")
			return
		}
		for i := 0; i < expectedJobs; i++ {
			jr, err := mb.Publish(struct{}{})
			if err != nil || jr == nil {
				t.Errorf("Publish() is failed")
				return
			}
		}
		err = mb.Shutdown()
		if err != nil {
			t.Errorf("Shutdown failed")
		}
		if expectedJobs != processedJobs {
			t.Errorf("Expected %d jobs to be processed but got %d", expectedJobs, processedJobs)
			return
		}
	})
}

func TestMicroBatchImplementation_Publish_ProcessSingleMessage(t *testing.T) {
	batchSize := 5
	frequencyMs := 200
	expectedJobs := 1
	processedJobs := 0
	mock := &mockBatchProcessor{process: func(batch []Job) error {
		fmt.Printf("Called batch processing with %v jobs, time is %s\n", len(batch), time.Now())
		processedJobs += len(batch)
		return nil
	}}
	t.Run("Should create an incomplete batch is there is no more messages provided", func(t *testing.T) {
		mb, err := Create(batchSize, frequencyMs, mock)
		if err != nil {
			t.Errorf("Create() is failed")
			return
		}
		jr, err := mb.Publish(struct{}{})
		if err != nil || jr == nil {
			t.Errorf("Publish() is failed")
			return
		}
		time.Sleep(time.Duration(frequencyMs*2) * time.Millisecond)
		err = mb.Shutdown()
		if err != nil {
			t.Errorf("Shutdown failed")
		}
		if expectedJobs != processedJobs {
			t.Errorf("Expected %d jobs to be processed but got %d", expectedJobs, processedJobs)
			return
		}
	})
}

func closeEnough(start, end time.Time) bool {
	diff := end.Sub(start).Milliseconds()
	if diff < 0 {
		diff = -diff
	}
	return diff < 50
}
