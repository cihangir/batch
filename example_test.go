package batch_test

import (
	"context"
	"fmt"
	"time"

	"github.com/cihangir/batch"
)

// This example demonstrates how to use the batch with a simple consume,
// producer workflow.
func ExampleNew() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		size            = 10
		interval        = 1 * time.Second
		counter         = 0
		processFinished = make(chan struct{})
	)

	b, err := batch.New[int](batch.WithSize(size), batch.WithInterval(interval))
	if err != nil {
		fmt.Printf("invalid response: got err: %v", err)
	}

	// consumer
	go func() {
		_ = b.Process(ctx, func(ctx context.Context, batch []int) error {
			counter += len(batch)
			return nil
		})
		close(processFinished) // signal we're done
	}()

	// producer
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			// add and wait until process is finished
			if err := b.Add(ctx, i); err != nil {
				fmt.Printf("invalid process response: got err: %v", err)
			}
		}()
	}

	// wait for the consumer to finish
	<-processFinished

	if err := b.Close(); err != nil {
		fmt.Printf("invalid error: got: %v", err)
	}

	fmt.Printf("Counter: %d", counter)

	// Output:
	// Counter: 10
}
