package receiver

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/marketfeed"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/aarityatech/tickerfeed/api/brokerrpc"
)

const maxSpin = 100

var meter = otel.Meter("feedserver/receiver")

// Config is the configuration for the gRPC receiver.
type Config struct {
	Addrs         []string      `mapstructure:"addrs"`
	ServiceConfig string        `mapstructure:"service_config" default:"{}"`
	MinInterval   time.Duration `mapstructure:"min_interval" default:"500ms"`
}

type Receiver struct {
	cfg Config

	// last quote for each instrument.
	quotesMu sync.RWMutex
	quotes   map[int]*atomic.Pointer[lastQuote]

	// subscription count for each instrument.
	subs     map[int]bool
	subUnsub chan *brokerrpc.StreamRequest

	// open-telemetry metrics
	spinYield         metric.Int64Counter
	spinCount         metric.Int64Counter
	brokerLatency     metric.Int64Histogram
	messageReceived   metric.Int64Counter
	subscriptionCount metric.Int64UpDownCounter
	streamReconnect   metric.Int64Counter
}

func New(cfg Config, ids []int) *Receiver {
	topics := make(map[int]*atomic.Pointer[lastQuote], len(ids))
	for _, id := range ids {
		entry := &atomic.Pointer[lastQuote]{}
		entry.Store(&lastQuote{
			id:     id,
			hasVal: false,
		})
		topics[id] = entry
	}

	rec := &Receiver{
		cfg:      cfg,
		quotes:   topics,
		subs:     make(map[int]bool),
		subUnsub: make(chan *brokerrpc.StreamRequest, 65536),
	}
	rec.createMeasures()
	return rec
}

// Read returns the last quote for the given instrument ids. If value is not
// available for the given instrument, it will be skipped. The returned slice
// will have the same order as the given ids but may have fewer elements.
func (recv *Receiver) Read(ids []int) []marketfeed.Packet {
	recv.quotesMu.RLock()
	defer recv.quotesMu.RUnlock()

	var quotes []marketfeed.Packet
	for _, id := range ids {
		entry, ok := recv.quotes[id]
		if !ok {
			continue
		}

		val := entry.Load()
		if val.hasVal {
			quotes = append(quotes, val.val)
		}
	}
	return quotes
}

// Receive starts the gRPC receiver. It will reconnect if the connection is lost.
// Everytime a batch of quotes is received, the onReceive callback is called if
// that instrument is subscribed.
func (recv *Receiver) Receive(ctx context.Context, onReceive func(quotes []marketfeed.Packet)) error {
	// register the custom resolver scheme
	const multiIPScheme = "multi-ip"
	var addrs []resolver.Address
	for _, addr := range recv.cfg.Addrs {
		addrs = append(addrs, resolver.Address{Addr: addr})
	}
	resolverBuilder := manual.NewBuilderWithScheme(multiIPScheme)
	resolverBuilder.InitialState(resolver.State{Addresses: addrs})
	resolver.Register(resolverBuilder)

	conn, err := grpc.DialContext(ctx,
		fmt.Sprintf("%s:///", multiIPScheme),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(recv.cfg.ServiceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                1 * time.Minute,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Info(ctx, "connected to broker", log.Str("target", conn.Target()))

	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			if err := recv.runStream(ctx, conn, onReceive); err != nil {
				log.Warn(ctx, "stream exited with error (will reconnect)", log.Err(err))
			}
			recv.streamReconnect.Add(ctx, 1)
		}
	}
}

// SubUnsub subscribes and unsubscribes to the given instrument ids. This should be
// called with any new-subscriptions or un-subscriptions.
func (recv *Receiver) SubUnsub(sub, unsub []int) {
	if len(sub) == 0 && len(unsub) == 0 {
		return
	}

	rpcReq := &brokerrpc.StreamRequest{}
	for _, id := range sub {
		rpcReq.Subscribe = append(rpcReq.Subscribe, int64(id))
	}
	for _, id := range unsub {
		rpcReq.Unsubscribe = append(rpcReq.Unsubscribe, int64(id))
	}

	recv.subUnsub <- rpcReq
}

func (recv *Receiver) runStream(ctx context.Context, conn grpc.ClientConnInterface, onReceive func(quotes []marketfeed.Packet)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	streamCl, err := brokerrpc.NewFeedBrokerClient(conn).StreamQuotes(ctx)
	if err != nil {
		return err
	}

	// subscribe to all quotes we are already subscribed to. this is needed
	// in case we are reconnecting.
	topics := recv.allSubscribedTopics()
	if len(topics) > 0 {
		if err := streamCl.Send(&brokerrpc.StreamRequest{
			Subscribe: topics,
		}); err != nil {
			return err
		}
	}

	go func() {
		defer cancel() // no point in living if the request pump is dead.

		if err := recv.requestPump(ctx, streamCl); err != nil {
			log.Error(ctx, "request-pump exited", log.Err(err))
		}
	}()

	pos := 0
	quotes := make([]marketfeed.Packet, 65536)
	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			batch, err := streamCl.Recv()
			if err != nil {
				return err
			}

			recvAt := time.Now().UnixMilli()
			for _, q := range batch.GetQuotes() {
				recv.brokerLatency.Record(ctx, recvAt-q.GetTimestamp().AsTime().UnixMilli())
				recv.messageReceived.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "ok")))

				pkt, err := marketfeed.ParseKambalaUnifiedPacket(q.GetQuoteData())
				if err != nil {
					recv.messageReceived.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "invalid")))
					continue
				}
				pkt.PacketReceivedAt = recvAt
				pkt.DerivedFuturePrice = q.GetDerivedFuturePrice()

				// Quote Data is kambala packet which does not have our instrument id.
				// So we need to set it here.
				pkt.InstrumentID = q.GetInstrumentId()

				quotes[pos] = pkt
				pos++
			}

			recv.putPackets(quotes[:pos], onReceive)
			pos = 0
		}
	}
}

func (recv *Receiver) requestPump(ctx context.Context, cl brokerrpc.FeedBroker_StreamQuotesClient) error {
	rpcReq := &brokerrpc.StreamRequest{}

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-tick.C:
			if len(rpcReq.Subscribe) == 0 && len(rpcReq.Unsubscribe) == 0 {
				continue
			}

			if err := cl.Send(rpcReq); err != nil {
				return err
			}

			for _, id := range rpcReq.Subscribe {
				recv.subs[int(id)] = true
			}
			for _, id := range rpcReq.Unsubscribe {
				delete(recv.subs, int(id))
			}
			rpcReq.Subscribe = rpcReq.Subscribe[:0]
			rpcReq.Unsubscribe = rpcReq.Unsubscribe[:0]

		case req := <-recv.subUnsub:
			rpcReq.Subscribe = append(rpcReq.Subscribe, req.Subscribe...)
			rpcReq.Unsubscribe = append(rpcReq.Unsubscribe, req.Unsubscribe...)
		}
	}
}

func (recv *Receiver) putPackets(quotes []marketfeed.Packet, onReceive func([]marketfeed.Packet)) {
	needToNotify := make([]marketfeed.Packet, 0, len(quotes))
	for _, pkt := range quotes {
		finalVal, notify := recv.atomicSpinUpdate(int(pkt.InstrumentID), func(cur *lastQuote) *lastQuote {
			if pkt.PacketReceivedAt < cur.val.PacketReceivedAt {
				// we already have a newer quote.
				return nil
			}

			newTop := cur.clone()
			newTop.val = pkt
			newTop.hasVal = true
			return newTop
		})

		if finalVal != nil && notify {
			needToNotify = append(needToNotify, finalVal.val)
		}
	}

	if len(needToNotify) > 0 {
		onReceive(needToNotify)
	}
}

// allSubscribedTopics returns all the topics we are subscribed to.
func (recv *Receiver) allSubscribedTopics() []int64 {
	var topics []int64
	for id := range recv.subs {
		topics = append(topics, int64(id))
	}
	return topics
}

func (recv *Receiver) atomicSpinUpdate(id int, apply func(cur *lastQuote) (new *lastQuote)) (*lastQuote, bool) {
	recv.quotesMu.RLock()
	entry, ok := recv.quotes[id]
	recv.quotesMu.RUnlock()

	if !ok {
		return nil, false
	}

	spinCount := 0
	for {
		curVal := entry.Load()
		newVal := apply(curVal)
		if newVal == nil || newVal == curVal {
			return curVal, false // no change.
		}

		notify := false
		t := time.Now()
		if t.Sub(curVal.lastNotify) > recv.cfg.MinInterval {
			newVal.lastNotify = t
			notify = true
		}

		if entry.CompareAndSwap(curVal, newVal) {
			return newVal, notify // success.
		} else {
			spinCount++
			recv.spinCount.Add(context.Background(), 1)
			if spinCount >= maxSpin {
				// too many spins can mean we are in a live-lock.
				// yield the CPU and try again later.
				spinCount = 0
				recv.spinYield.Add(context.Background(), 1)
				runtime.Gosched()
			}
		}
	}
}

func (recv *Receiver) createMeasures() {
	var err error

	recv.streamReconnect, err = meter.Int64Counter("quote_stream.reconnects")
	if err != nil {
		otel.Handle(err)
	}

	recv.messageReceived, err = meter.Int64Counter("quote_stream.quotes_received")
	if err != nil {
		otel.Handle(err)
	}

	recv.brokerLatency, err = meter.Int64Histogram("quote_stream.broker_latency", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}

	recv.subscriptionCount, err = meter.Int64UpDownCounter("quote_stream.subscriptions")
	if err != nil {
		otel.Handle(err)
	}

	recv.spinYield, err = meter.Int64Counter("broadcast.atomic.spin_yield")
	if err != nil {
		otel.Handle(err)
	}

	recv.spinCount, err = meter.Int64Counter("broadcast.atomic.spin_count")
	if err != nil {
		otel.Handle(err)
	}
}
