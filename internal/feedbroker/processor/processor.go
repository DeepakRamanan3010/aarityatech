package processor

import (
	"context"
	"time"

	"github.com/aarityatech/pkg/instrumentlut"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
)

var meter = otel.GetMeterProvider().Meter("feedbroker/processor")

type LUT interface {
	ByID(id int) (instrumentlut.Instrument, bool)
	AllIDs() []int
}

// Processor implements quote pre-processing logic.
type Processor struct {
	cfg   Config
	dfc   *futureCalculator
	lut   LUT
	cache quoteCache

	// open-telemetry metrics
	quotesDuration metric.Float64Histogram
}

type quoteCache interface {
	Get(keys []int) []receiver.Quote
}

func New(cfg Config, cache quoteCache, lut LUT) (*Processor, error) {
	proc := &Processor{
		cfg:   cfg,
		lut:   lut,
		cache: cache,
	}
	proc.createMeasures()

	if cfg.DerivedFuture {
		proc.dfc = newFutureCalculator(cache, lut, time.Now)
	}

	return proc, nil
}

func (proc *Processor) Process(quote receiver.Quote) []receiver.Quote {
	t := time.Now()
	defer func() {
		proc.quotesDuration.Record(context.Background(), float64(time.Since(t).Microseconds()))
	}()

	if proc.dfc != nil {
		return proc.dfc.compute(quote)
	}

	// return the quote as-is.
	return []receiver.Quote{quote}
}

// Run starts all background processing threads that the processor needs.
func (proc *Processor) Run(ctx context.Context) error {
	ticker := time.NewTimer(0) // start immediately
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			ticker.Reset(proc.cfg.RunInterval)

			if proc.dfc != nil {
				proc.dfc.computeClosest()
			}
		}
	}
}

func (proc *Processor) createMeasures() {
	var err error

	proc.quotesDuration, err = meter.Float64Histogram("processor.duration", metric.WithUnit("us"))
	if err != nil {
		otel.Handle(err)
	}
}
