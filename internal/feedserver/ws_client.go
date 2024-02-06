package feedserver

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/marketfeed"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

type wsClient struct {
	ctx    context.Context
	conn   *websocket.Conn
	closed atomic.Bool

	mu    sync.RWMutex
	msg   messageEnvelope
	modes map[int]int
}

func newWSClient(ctx context.Context, conn *websocket.Conn) *wsClient {
	ctx = log.NewCtx(ctx, log.Str("remote_addr", conn.RemoteAddr().String()))

	wc := &wsClient{
		ctx:   ctx,
		conn:  conn,
		modes: make(map[int]int),
	}
	wc.msg = allocMessageEnvelope(func(buf []byte) {
		wc.write(websocket.BinaryMessage, buf)
	})
	return wc
}

func (wc *wsClient) apply(req wsRequest) (subs, unSubs []int) {
	wc.mu.Lock()
	for _, update := range req.FeedUpdates {
		for _, instrument := range update.Instruments {
			if update.Mode == modeUnsub {
				unSubs = append(unSubs, instrument)
			} else {
				subs = append(subs, instrument)
			}
			wc.modes[instrument] = update.Mode
		}
	}
	wc.mu.Unlock()
	return subs, unSubs
}

func (wc *wsClient) put(quotes []marketfeed.Packet) {
	if len(quotes) == 0 || wc.closed.Load() {
		return
	}

	wc.mu.Lock()
	for _, quote := range quotes {
		wc.msg.addPacket(quote, wc.modes[int(quote.InstrumentID)])
	}
	wc.msg.flush()
	wc.mu.Unlock()
}

func (wc *wsClient) write(opcode websocket.MessageType, msg []byte) {
	if wc.closed.Load() {
		return
	}

	err := wc.conn.WriteMessage(opcode, msg)
	if err != nil {
		wc.conn.CloseAndClean(err)
		wc.closed.Store(true)
	}
}

func errMsg(code string) []byte {
	d, _ := json.Marshal(map[string]string{"e": code})
	return d
}
