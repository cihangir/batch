package batch

import "time"

// Option defines the signature of a batch option.
type Option func(*config) error

var (
	// DefaultSize is the default size of a batch.
	DefaultSize = 10

	// DefaultInterval is the default interval of a batch.
	DefaultInterval = 100 * time.Millisecond
)

type config struct {
	// user config
	size     int
	interval time.Duration
}

// WithInterval sets the interval of a batch processing deadline.
func WithInterval(interval time.Duration) Option {
	return func(c *config) error {
		c.interval = interval
		return nil
	}
}

// WithSize sets the size of a batch.
func WithSize(size int) Option {
	return func(c *config) error {
		c.size = size
		return nil
	}
}
