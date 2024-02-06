package feedserver

import (
	"context"

	"github.com/aarityatech/pkg/instrumentlut"
	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/objstore"
	"github.com/aarityatech/pkg/telemetry"

	"github.com/aarityatech/tickerfeed/internal/feedserver/receiver"
)

// Config is the configuration for the websocket feed-server.
type Config struct {
	Addr            string `mapstructure:"addr" default:":8080"`
	AntPoolSize     int    `mapstructure:"ant_pool_size" default:"10000"`
	ReadBufferSize  int    `mapstructure:"read_buf_size" default:"4096"`
	MaxWriteBufSize int    `mapstructure:"max_write_buf_size" default:"8192"`

	ValidateRequest bool   `mapstructure:"validate_request" default:"false"`
	AuthPublicKeyID string `mapstructure:"auth_public_key_id"`
	AuthPublicKey   string `mapstructure:"auth_public_key"`

	Telemetry     telemetry.Config     `mapstructure:"telemetry"`
	GRPCReceiver  receiver.Config      `mapstructure:"grpc_receiver"`
	InstrumentLUT instrumentlut.Config `mapstructure:"instrument_lut"`
}

// Serve starts the websocket feed-server. It blocks until the server is stopped.
// Context cancellation will shut down the server gracefully.
func Serve(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objStore, err := objstore.NewS3Store(ctx, objstore.S3Config{Region: "ap-south-1"})
	if err != nil {
		return err
	}

	lut, err := instrumentlut.Setup(ctx, cfg.InstrumentLUT, objStore)
	if err != nil {
		return err
	}

	recv := receiver.New(cfg.GRPCReceiver, lut.AllIDs())

	wss, err := newServer(ctx, cfg, recv)
	if err != nil {
		return err
	}

	go func() {
		defer cancel() // no point in living if the receiver is dead.

		if err := recv.Receive(ctx, wss.onReceive); err != nil {
			log.Error(ctx, "gRPC receiver exited", log.Err(err))
		}
	}()

	return wss.Serve(ctx)
}
