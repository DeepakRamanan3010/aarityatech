package feedbroker

import (
	"context"
	"time"

	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/server"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/aarityatech/tickerfeed/api/brokerrpc"
	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
)

var meter = otel.Meter("feedbroker/grpc-server")

type brokerServer struct {
	brokerrpc.UnimplementedFeedBrokerServer

	cache      receiver.Cache
	flushEvery time.Duration

	// open-telemetry metrics
	sendCounter     metric.Int64Counter
	recvCounter     metric.Int64Counter
	activeStreams   metric.Int64UpDownCounter
	dispatchLatency metric.Float64Histogram
}

func newBrokerServer(cache receiver.Cache, flushEvery time.Duration) *brokerServer {
	if flushEvery == 0 {
		flushEvery = 50 * time.Millisecond
	}

	br := &brokerServer{
		cache:      cache,
		flushEvery: flushEvery,
	}
	br.createMeasures()
	return br
}

// Serve starts the feedbroker gRPC server.
func (broker *brokerServer) Serve(ctx context.Context, addr string) error {
	stdOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 24 * time.Hour,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	srv := grpc.NewServer(stdOpts...)
	reflection.Register(srv)
	srv.RegisterService(&brokerrpc.FeedBroker_ServiceDesc, broker)

	log.Info(ctx, "starting feedbroker gRPC server", log.Str("addr", addr))
	return server.Serve(ctx,
		server.WithGRPCTarget(addr, srv),
	)
}

func (broker *brokerServer) StreamQuotes(stream brokerrpc.FeedBroker_StreamQuotesServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	broker.activeStreams.Add(ctx, 1)
	defer broker.activeStreams.Add(ctx, -1)

	cc := &clientConn{
		subs:        make(map[int]int64),
		reqs:        make(chan *brokerrpc.StreamRequest, 100),
		cache:       broker.cache,
		broker:      broker,
		stream:      stream,
		minInterval: broker.flushEvery.Milliseconds(),
	}

	go func() {
		defer cancel()
		if err := cc.runReader(ctx); err != nil {
			log.Warn(ctx, "reader exited", log.Err(err))
		}
	}()

	return cc.runWriter(ctx)
}

func (broker *brokerServer) createMeasures() {
	var err error

	broker.sendCounter, err = meter.Int64Counter("quote_stream.quotes_sent")
	if err != nil {
		otel.Handle(err)
	}

	broker.recvCounter, err = meter.Int64Counter("quote_stream.requests_received")
	if err != nil {
		otel.Handle(err)
	}

	broker.activeStreams, err = meter.Int64UpDownCounter("quote_stream.active.count")
	if err != nil {
		otel.Handle(err)
	}

	broker.dispatchLatency, err = meter.Float64Histogram("quote_stream.dispatch_latency", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}
}
