package feedbroker

import (
	"context"
	"time"

	"github.com/aarityatech/pkg/instrumentlut"
	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/objstore"
	"github.com/aarityatech/pkg/telemetry"

	"github.com/aarityatech/tickerfeed/internal/feedbroker/processor"
	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
	"github.com/aarityatech/tickerfeed/pkg/precache"
)

// Config is the configuration for the feedbroker server.
type Config struct {
	Addr          string        `mapstructure:"addr" default:":8080"`
	FlushInterval time.Duration `mapstructure:"flush_interval" default:"50ms"`

	Receiver      receiver.Config      `mapstructure:"receiver"`
	Telemetry     telemetry.Config     `mapstructure:"telemetry"`
	Processor     processor.Config     `mapstructure:"processor"`
	InstrumentLUT instrumentlut.Config `mapstructure:"instrument_lut"`
}

// Serve starts the feedbroker server.
func Serve(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lut, err := setupLUT(ctx, cfg)
	if err != nil {
		return err
	}

	quoteCache := precache.New[receiver.Quote](lut.AllIDs())

	proc, err := processor.New(cfg.Processor, quoteCache, lut)
	if err != nil {
		return err
	}

	rec, err := receiver.New(cfg.Receiver, lut, quoteCache, proc)
	if err != nil {
		return err
	}

	// start pre-processor instance.
	go func() {
		defer cancel() // no point in continuing if we can't pre-process. kill all.

		if err := proc.Run(ctx); err != nil {
			log.Warn(ctx, "processor exited", log.Err(err))
		}
	}()

	// start UDP receiver instance.
	go func() {
		defer cancel() // no point in continuing if we can't listen to UDP. kill all.

		if err := rec.Run(ctx); err != nil {
			log.Warn(ctx, "udp receiver exited", log.Err(err))
		}
	}()

	grpcSrv := newBrokerServer(quoteCache, cfg.FlushInterval)
	return grpcSrv.Serve(ctx, cfg.Addr)
}

func setupLUT(ctx context.Context, cfg Config) (*instrumentlut.InstrumentLUT, error) {
	objStore, err := objstore.NewS3Store(ctx, objstore.S3Config{Region: "ap-south-1"})
	if err != nil {
		return nil, err
	}
	return instrumentlut.Setup(ctx, cfg.InstrumentLUT, objStore)
}
