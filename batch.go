package batch

import (
	"context"
	"errors"
	"time"
)

// ErrBatchClosed indicates that the Batch is closed for new additions.
var ErrBatchClosed = errors.New("closed")

// Batch holds the input and output channels which are used to process the batch.
type Batch[T any] struct {
	// control structures
	closeChan chan struct{}

	// user config
	size     int
	interval time.Duration

	// input channel transfers items to the batcher
	input chan inputEnvelope[T]

	// output channel transfers items to the processor when the "batch" is
	// ready either the max size is reached or the timeout is reached
	output chan outputEnvelope[T]
}

// ProcessFn defines the function signature for the processor.
type ProcessFn[T any] func(ctx context.Context, batch []T) error

// New creates a new batch processor. Size indicates the maximum number of items
// that a batch could hold. interval indicates the maximum time a batch can wait
// to be filled with items.
func New[T any](options ...Option) (*Batch[T], error) {
	c := &config{
		size:     DefaultSize,
		interval: DefaultInterval,
	}
	for _, o := range options {
		if err := o(c); err != nil {
			return nil, err
		}
	}

	b := &Batch[T]{
		closeChan: make(chan struct{}),
		size:      c.size,
		interval:  c.interval,
		input:     make(chan inputEnvelope[T]),
		output:    make(chan outputEnvelope[T]),
	}

	go b.processor()

	return b, nil
}

// Close closes the batch and stops the processor.
func (b *Batch[T]) Close() error {
	close(b.closeChan)
	return nil
}

// Add adds item to the batch. Returns an error if the batch is closed. Add
// blocks until the item is processed. Returns the error that happened during
// the process to the caller.
//
// Common behaviors:
//
// Add would block after "size" many items are added, if the processor has not
// been initialized yet.
//
// Items that are successfully added to the batch are always processed.
// Cancelling the context or closing the batch wont affect them.
func (b *Batch[T]) Add(ctx context.Context, item T) error {
	// The check here is to make sure that we exit early if the batch is closed.
	// If there are multiple readily available channels at any given point, Go
	// will select a random one. We want to prioritize the close operation over
	// everything.
	//
	// These kind of try operations are cheap/efficient with the help of Go
	// compiler.
	// We also have the similar check down below.
	select {
	case <-b.closeChan:
		return ErrBatchClosed
	default:
	}

	// create the err chan outside of the struct so it can be read from
	errChan := make(chan error)
	inputEnvelope := inputEnvelope[T]{
		item:    item,
		errChan: errChan,
	}

	select {
	case <-b.closeChan:
		return ErrBatchClosed
	case <-ctx.Done():
		return ctx.Err()
	case b.input <- inputEnvelope:
		return <-errChan
	}
}

// Go adds item to the batch. Returns an error if the batch is closed. Go does
// not block and does not wait for message to be processed.
//
// See Add function for "Common behaviors"
func (b *Batch[T]) Go(ctx context.Context, item T) error {
	// See Add function for the explanation of this check.
	select {
	case <-b.closeChan:
		return ErrBatchClosed
	default:
	}

	inputEnvelope := inputEnvelope[T]{
		item:    item,
		errChan: make(chan<- error),
	}

	select {
	case <-b.closeChan:
		return ErrBatchClosed
	case <-ctx.Done():
		return ctx.Err()
	case b.input <- inputEnvelope:
		return nil
	}
}

// Process accepts a function that processes the batch. The function blocks
// until a batch can be processed. The function needs to be called after each
// batch is processed. Errors happening during the process are returned to the
// interested adder.
func (b *Batch[T]) Process(ctx context.Context, fn ProcessFn[T]) error {
	// See Add function for the explanation of this check.
	select {
	case <-b.closeChan:
		return ErrBatchClosed
	default:
	}

	select {
	// we don't need to listen closeChan here because we need to consume the
	// buffer. output channel would be closed if the batch is closed and buffer
	// is consumed.
	case <-ctx.Done():
		return ctx.Err()
	case batch, ok := <-b.output:
		if !ok {
			return ErrBatchClosed
		}
		err := fn(ctx, batch.items)
		if err != nil {
			for _, errChan := range batch.errChans {
				errChan <- err
			}
		}
		for _, errChan := range batch.errChans {
			close(errChan)
		}
		return err
	}
}

func (b *Batch[T]) processor() {
	var itemBuffer []T
	var errChans []chan<- error

	ticker := time.NewTicker(b.interval)

	reset := func(resetInterval bool) {
		itemBuffer = make([]T, 0, b.size)
		errChans = make([]chan<- error, 0, b.size)
		if resetInterval {
			ticker.Reset(b.interval)
		}
	}

	closeHook := func() {
		if len(itemBuffer) > 0 {
			b.output <- outputEnvelope[T]{
				items:    itemBuffer,
				errChans: errChans,
			}
		}
		close(b.input)
		close(b.output)
	}

	reset(false)

	defer ticker.Stop()

	for {
		// See Add function for the explanation of this check.
		select {
		case <-b.closeChan:
			closeHook()
			return
		default:
		}

		select {
		case event := <-b.input:
			itemBuffer = append(itemBuffer, event.item)
			errChans = append(errChans, event.errChan)

			if len(itemBuffer) == b.size {
				b.output <- outputEnvelope[T]{
					items:    itemBuffer,
					errChans: errChans,
				}

				reset(true)
			}
		case <-ticker.C:
			if len(itemBuffer) > 0 {
				b.output <- outputEnvelope[T]{
					items:    itemBuffer,
					errChans: errChans,
				}

				reset(true)
			}
		case <-b.closeChan:
			closeHook()
			return
		}
	}

}

type inputEnvelope[T any] struct {
	item    T
	errChan chan<- error
}

type outputEnvelope[T any] struct {
	items    []T
	errChans []chan<- error
}
