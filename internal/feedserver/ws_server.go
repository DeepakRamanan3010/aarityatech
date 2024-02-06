package feedserver

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/marketfeed"
	"github.com/aarityatech/pkg/token"
	"github.com/lesismal/nbio/nbhttp"
	"github.com/lesismal/nbio/nbhttp/websocket"
	"github.com/panjf2000/ants/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const shardSize = 256

var meter = otel.GetMeterProvider().Meter("feedserver/ws")

// Receiver is responsible for receiving quotes from the broker.
type Receiver interface {
	// Read should return the latest quotes for the given instrument IDs.
	// If the instrument ID is not found, it should skip it.
	Read(ids []int) []marketfeed.Packet

	// Receive should run the goroutine to receive quotes from the broker.
	// It should call the onReceive callback for each batch of quotes received.
	// If context is cancelled, it should return immediately.
	Receive(ctx context.Context, onReceive func(quotes []marketfeed.Packet)) error

	// SubUnsub should subscribe/unsubscribe to the given instrument IDs
	// with the broker.
	SubUnsub(sub, unsub []int)
}

type wsServer struct {
	ctx      context.Context
	verifier *token.Verifier

	// internal state
	engine   *nbhttp.Engine
	upgrader *websocket.Upgrader
	receiver Receiver
	shards   [shardSize]topicShard
	antPool  *ants.Pool

	// open-telemetry metrics
	wsUpgrades   metric.Int64Counter
	wsMessages   metric.Int64Counter
	wsConnsCount metric.Int64UpDownCounter
}

type topicShard struct {
	sync.RWMutex
	subs map[int][]*wsClient
}

func newServer(ctx context.Context, cfg Config, recv Receiver) (*wsServer, error) {
	ap, err := ants.NewPool(cfg.AntPoolSize)
	if err != nil {
		return nil, err
	}

	ws := &wsServer{
		ctx:      ctx,
		antPool:  ap,
		receiver: recv,
	}
	ws.createMeasures()

	if cfg.ValidateRequest {
		if cfg.AuthPublicKey == "" {
			return nil, errors.New("auth public key must be set when validate_request=true")
		}

		keys := map[string]string{cfg.AuthPublicKeyID: cfg.AuthPublicKey}
		verifier, err := token.NewVerifier(keys)
		if err != nil {
			return nil, err
		}
		ws.verifier = verifier
	}

	ws.upgrader = websocket.NewUpgrader()
	ws.upgrader.OnOpen(ws.onOpen)
	ws.upgrader.OnClose(ws.onClose)
	ws.upgrader.OnMessage(ws.onMessage)
	ws.upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	ws.upgrader.KeepaliveTime = 2 * time.Hour
	ws.upgrader.BlockingModAsyncWrite = true

	ws.engine = nbhttp.NewEngine(nbhttp.Config{
		Context:                 ctx,
		Network:                 "tcp",
		Addrs:                   []string{cfg.Addr},
		IOMod:                   nbhttp.IOModMixed,
		MaxLoad:                 1000000,
		Handler:                 ws.handler(),
		KeepaliveTime:           2 * time.Hour,
		ReadBufferSize:          cfg.ReadBufferSize,
		MaxBlockingOnline:       50000,
		MaxWriteBufferSize:      cfg.MaxWriteBufSize,
		ReleaseWebsocketPayload: true,
	})

	return ws, nil
}

// Serve starts the websocket feed-server. It blocks until the server is stopped.
func (wss *wsServer) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wss.ctx = ctx
	wss.engine.Context = ctx

	if err := wss.engine.Start(); err != nil {
		return err
	}

	<-ctx.Done()
	wss.engine.Stop()
	return nil
}

func (wss *wsServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1", http.HandlerFunc(wss.v1Websocket))
	mux.Handle("/health", http.HandlerFunc(wss.health))

	return otelhttp.NewHandler(mux, "feedserver",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)
}

func (wss *wsServer) v1Websocket(w http.ResponseWriter, r *http.Request) {
	if wss.verifier != nil {
		q := r.URL.Query()
		clientID := strings.TrimSpace(q.Get("client_id"))
		clientVer := strings.TrimSpace(q.Get("client_version"))
		authToken := strings.TrimSpace(q.Get("auth_token"))

		if authToken == "" || clientID == "" || clientVer == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_, err := wss.verifier.VerifyAccessToken(authToken, false)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	conn, err := wss.upgrader.Upgrade(w, r, nil)
	if err != nil {
		wss.wsUpgrades.Add(wss.ctx, 1, metric.WithAttributes(attribute.String("status", "failed")))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// initialize the client and set it as the session. and return from this
	// function to release the net/http allocated resources.
	wc := newWSClient(wss.ctx, conn)
	conn.SetSession(wc)
	wss.wsUpgrades.Add(wss.ctx, 1, metric.WithAttributes(attribute.String("status", "ok")))
	wss.wsConnsCount.Add(wss.ctx, 1)
}

func (wss *wsServer) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (wss *wsServer) onOpen(conn *websocket.Conn) {
	wss.wsConnsCount.Add(wss.ctx, 1)
}

// onClose is called when a websocket connection is closed.
func (wss *wsServer) onClose(conn *websocket.Conn, err error) {
	wss.wsConnsCount.Add(wss.ctx, -1)

	wc, ok := conn.Session().(*wsClient)
	if !ok || wc == nil {
		return
	}

	for topic := range wc.modes {
		wss.unsubscribe(topic, wc)
	}
	conn.SetSession(nil)
	wc.closed.Store(true)
}

// onMessage is called when a websocket message is received.
func (wss *wsServer) onMessage(conn *websocket.Conn, messageType websocket.MessageType, bytes []byte) {
	wss.wsMessages.Add(wss.ctx, 1, metric.WithAttributes(attribute.Int("type", int(messageType))))

	if messageType != websocket.TextMessage {
		return
	}

	wc, ok := conn.Session().(*wsClient)
	if !ok {
		return
	}

	var req wsRequest
	if err := req.unmarshal(bytes); err != nil {
		wc.write(websocket.TextMessage, errMsg(err.Error()))
		return
	}

	if err := wss.antPool.Submit(func() {
		subs, unSubs := wc.apply(req)

		// read the latest quotes for the newly subscribed topics
		// and send them to the client.
		wc.put(wss.receiver.Read(subs))

		// subscribe/unsubscribe to the broker.
		for _, topic := range subs {
			wss.subscribe(topic, wc)
		}
		for _, topic := range unSubs {
			wss.unsubscribe(topic, wc)
		}
	}); err != nil {
		log.Warn(wss.ctx, "failed to submit to ant", log.Err(err))
	}
}

func (wss *wsServer) onReceive(quotes []marketfeed.Packet) {
	for _, quote := range quotes {
		id := int(quote.InstrumentID)
		for _, sub := range wss.getSubs(id) {
			sub.put([]marketfeed.Packet{quote})
		}
	}
}

func (wss *wsServer) getSubs(topic int) []*wsClient {
	shardIdx := wss.getShardIdx(topic)
	shard := &wss.shards[shardIdx]

	shard.RLock()
	subs := make([]*wsClient, len(shard.subs[topic]))
	copy(subs, shard.subs[topic])
	shard.RUnlock()

	return subs
}

func (wss *wsServer) subscribe(topic int, client *wsClient) {
	shardIdx := wss.getShardIdx(topic)

	shard := &wss.shards[shardIdx]
	shard.Lock()
	defer shard.Unlock()

	if shard.subs == nil {
		shard.subs = make(map[int][]*wsClient)
	}

	if len(shard.subs[topic]) == 0 {
		// first subscription to this topic. need to notify the receiver.
		wss.receiver.SubUnsub([]int{topic}, nil)
		shard.subs[topic] = []*wsClient{}
	}

	shard.subs[topic] = append(shard.subs[topic], client)
}

func (wss *wsServer) unsubscribe(topic int, client *wsClient) {
	shardIdx := wss.getShardIdx(topic)

	shard := &wss.shards[shardIdx]
	shard.Lock()
	defer shard.Unlock()

	subs := shard.subs[topic]
	for i, c := range subs {
		if c == client {
			if len(subs) == 1 {
				// last subscription to this topic. need to notify the receiver.
				wss.receiver.SubUnsub(nil, []int{topic})
				shard.subs[topic] = nil
			} else {
				// do a switcheroo and slice off the last element.
				subs[i] = subs[len(subs)-1]
				subs[len(subs)-1] = nil
				shard.subs[topic] = subs[:len(subs)-1]
			}
			return
		}
	}
}

func (wss *wsServer) getShardIdx(key int) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(key))
	hasher := fnv.New64a()
	_, err := hasher.Write(buf[:])
	if err != nil {
		panic(err) // should never happen
	}
	return hasher.Sum64() % shardSize
}

func (wss *wsServer) createMeasures() {
	var err error

	wss.wsUpgrades, err = meter.Int64Counter("ws.upgrades")
	if err != nil {
		otel.Handle(err)
	}

	wss.wsMessages, err = meter.Int64Counter("ws.messages")
	if err != nil {
		otel.Handle(err)
	}

	wss.wsConnsCount, err = meter.Int64UpDownCounter("ws.connections")
	if err != nil {
		otel.Handle(err)
	}
}
