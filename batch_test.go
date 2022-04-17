package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSize := 10
	totalItems := 100

	// make the deadline long so that we have time to process the batches
	batch, _ := New[int](WithSize(batchSize), WithInterval(10*time.Second))

	go func() {
		for i := 0; i < totalItems; i++ {
			_ = batch.Go(ctx, i)
		}
	}()

	count := 0

	for count != totalItems {
		_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
			if len(batch) != batchSize {
				t.Fatalf("invalid batch size: got: %d, want: %d", len(batch), batchSize)
			}
			count = count + len(batch)
			return nil
		})
	}
}

func TestMaxWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// make sure we the batch size is less than the added item count
	totalItems := 10

	batch, _ := New[int](WithSize(100), WithInterval(time.Second))

	go func() {
		for i := 0; i < totalItems; i++ {
			_ = batch.Go(ctx, i)
		}
	}()

	_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
		if len(batch) != totalItems {
			t.Fatalf("invalid batch size: got: %d, want: %d", len(batch), totalItems)
		}
		return nil
	})
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	totalItems := 10
	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))

	for i := 0; i < totalItems; i++ {
		go func(i int) { _ = batch.Add(ctx, i) }(i)
	}

	cancel()

	_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
		if len(batch) != totalItems {
			t.Fatalf("invalid batch size: got: %d, want: %d", len(batch), totalItems)
		}
		return nil
	})
}

func TestAddWithClosedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))
	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	if err, wanted := batch.Add(ctx, 1), ErrBatchClosed; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestAddWithLaterClosedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSize := 10
	// add one more item than the batch size, so that Add would block.
	totalItems := batchSize + 1
	batch, _ := New[int](WithSize(batchSize), WithInterval(10*time.Second))

	var wg sync.WaitGroup
	wg.Add(totalItems)

	for i := 0; i < totalItems; i++ {
		go func(i int) {
			wg.Done() // just to make sure the go routine is started
			_ = batch.Add(ctx, i)
		}(i)
	}
	// wait until we have a full batch and a waiting Add op.
	wg.Wait()

	// this will cause second select within Add function to block and cause
	// closeChan to be read
	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	if err, wanted := batch.Add(ctx, 1), ErrBatchClosed; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestAddWithClosedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))

	cancel()

	if err, wanted := batch.Add(ctx, 1), context.Canceled; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestGoWithClosedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))
	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	if err, wanted := batch.Go(ctx, 1), ErrBatchClosed; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestGoWithLaterClosedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSize := 10
	totalItems := batchSize + 1
	batch, _ := New[int](WithSize(batchSize), WithInterval(10*time.Second))

	var wg sync.WaitGroup
	wg.Add(totalItems)

	for i := 0; i < totalItems; i++ {
		go func(i int) {
			wg.Done() // just to make sure the go routine is started
			_ = batch.Go(ctx, i)
		}(i)
	}
	// wait until we have a full batch and a waiting Add op.
	wg.Wait()

	// this will cause second select within Add function to block and cause
	// closeChan to be read
	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	if err, wanted := batch.Go(ctx, 1), ErrBatchClosed; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestGoWithClosedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))

	cancel()

	if err, wanted := batch.Go(ctx, 1), context.Canceled; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestBatchWithItemsWithoutProcessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	batch, _ := New[int](WithSize(100), WithInterval(10*time.Second))

	for i := 0; i < 10; i++ {
		_ = batch.Go(ctx, i)
	}

	cancel()

	if err, wanted := batch.Add(ctx, 1), context.Canceled; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestAddWithProcessError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch, _ := New[int](WithSize(1), WithInterval(time.Second))
	var internalErr = errors.New("internal error")

	go func() {
		_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
			return internalErr
		})
	}()

	if err, wanted := batch.Add(ctx, 1), internalErr; err != wanted {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, wanted)
	}
}

func TestDrainItemBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a batch config that won't be processed till we close it.
	batch, _ := New[int](WithSize(10), WithInterval(time.Second))

	consumerStarted := make(chan struct{})
	processCalled := make(chan struct{})
	go func() {
		close(consumerStarted)
		// test we only call Process once. If the output channel is closed, we
		// shouldn't call Process again.
		_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
			return nil
		})
		close(processCalled)
	}()

	<-consumerStarted

	if err := batch.Go(ctx, 1); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	// make sure we call the processor if there are items waiting in the buffer.
	<-processCalled
}

func TestProcessWithClosedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch, _ := New[int](WithSize(1), WithInterval(time.Second))

	if err := batch.Close(); err != nil {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, nil)
	}

	err := batch.Process(ctx, func(ctx context.Context, batch []int) error {
		return nil
	})
	if err != ErrBatchClosed {
		t.Fatalf("invalid error: got: %v, wanted: %v", err, ErrBatchClosed)
	}
}

func Benchmark100Batch(b *testing.B) {
	batchCount := 100
	interval := 100 * time.Millisecond
	bench(b, batchCount, interval)
}

func Benchmark10Batch(b *testing.B) {
	batchCount := 10
	interval := 100 * time.Millisecond
	bench(b, batchCount, interval)
}

func Benchmark100ms(b *testing.B) {
	batchCount := 100
	interval := 100 * time.Millisecond
	bench(b, batchCount, interval)
}

func Benchmark1s(b *testing.B) {
	batchCount := 100
	interval := 1 * time.Second
	bench(b, batchCount, interval)
}

func bench(b *testing.B, batchCount int, interval time.Duration) {
	b.ReportAllocs()
	ctx, cancel := context.WithCancel(context.Background())
	batch, _ := New[int](WithSize(batchCount), WithInterval(interval))

	count := 0
	go func() {
		n := b.N / batchCount
		for i := 0; i < n; i++ {
			_ = batch.Process(ctx, func(ctx context.Context, batch []int) error {
				count = count + len(batch)
				return nil
			})
			if count == b.N {
				cancel()
				return
			}
		}
	}()
	for i := 0; i < b.N; i++ {
		_ = batch.Go(ctx, i)
	}
}
