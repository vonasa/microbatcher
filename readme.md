# Micro batch pipeline

This is a golang library implementing a micro batch pipeline. It allows for the application to publish jobs as they come along and package them into batches of a configurable size/frequency.

The batch processor implementation is a dependency.

# Requirement assumptions

To complete the requirements a couple of assumptions were made

- The publish function doesn't wait for the job to be picked up for a batch. It returns immediately
- If there's fewer jobs available than the batch size, the batch size will be smaller
- Batch processing errors are just logged
- Graceful shutdown for termination by the system is not implemented. 
- Shutdown will process all jobs according to the batch size/frequency requirements

# Codebase

- To run tests
```shell
make test
```

# Usage

- To configure and create an instance:
```go
mb, err := Create(batchSize, frequencyMs, batchProcessor)
if err != nil {
    fmt.println("Create() is failed. Please verify the arguments")
}
```

- To publish a message
```go
jr, err := mb.Publish(struct{}{})
if err != nil {
    fmt.println("Publish() is failed. Is the microbatcher is shut down?")
}
```

- To shut down and make sure that all remaining message
```go
err := mb.Shutdown()
if err != nil {
    fmt.println("Shutdown() is failed. Is the microbatcher already shut down?")
}

```
