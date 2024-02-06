package feedbroker

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aarityatech/tickerfeed/api/brokerrpc"
	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
)

type clientConn struct {
	broker      *brokerServer
	cache       receiver.Cache
	stream      brokerrpc.FeedBroker_StreamQuotesServer
	minInterval int64

	subs map[int]int64 // instrument-id --> timestamps from last quote.
	reqs chan *brokerrpc.StreamRequest
}

func (cc *clientConn) runWriter(ctx context.Context) error {
	timer := time.NewTicker(time.Duration(cc.minInterval) * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case req := <-cc.reqs:
			if req == nil {
				continue
			}
			cc.updateSubscriptions(req)
			if err := cc.writeIfNeeded(); err != nil {
				return err
			}

		case <-timer.C:
			if err := cc.writeIfNeeded(); err != nil {
				return err
			}
		}
	}
}

func (cc *clientConn) runReader(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			var msg brokerrpc.StreamRequest
			if err := cc.stream.RecvMsg(&msg); err != nil {
				cc.broker.recvCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
				return err
			}
			cc.broker.recvCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "ok")))

			select {
			case <-ctx.Done():
				return nil

			case cc.reqs <- &msg:
				// sent.
			}
		}
	}
}

func (cc *clientConn) writeIfNeeded() error {
	ids := make([]int, 0, len(cc.subs))
	for id := range cc.subs {
		ids = append(ids, id)
	}

	quotes := cc.cache.Get(ids)
	if len(quotes) == 0 {
		return nil
	}

	quBuf := make([]*brokerrpc.Quote, 4096)
	nextPos := 0
	for _, q := range quotes {
		curQuoteTime := q.Time.UnixMilli()
		lastQuoteTime := cc.subs[q.ID]
		if curQuoteTime-lastQuoteTime < cc.minInterval {
			// we sent a quote for this instrument recently.
			continue
		}

		cc.subs[q.ID] = curQuoteTime
		quBuf[nextPos] = &brokerrpc.Quote{
			InstrumentId:       int64(q.ID),
			QuoteData:          q.Data,
			Timestamp:          timestamppb.New(q.Time),
			DerivedFuturePrice: q.DerFut,
		}
		nextPos++

		if nextPos >= len(quBuf) {
			// buffer is full. send it.

			if err := cc.sendBatch(quBuf[:nextPos]); err != nil {
				return err
			}
			nextPos = 0
		}
	}

	// send the remaining.
	if nextPos > 0 {
		return cc.sendBatch(quBuf[:nextPos])
	}
	return nil
}

func (cc *clientConn) sendBatch(quotes []*brokerrpc.Quote) error {
	for _, quote := range quotes {
		cc.broker.dispatchLatency.Record(context.Background(),
			float64(time.Since(quote.Timestamp.AsTime()).Milliseconds()))
	}

	err := cc.stream.Send(&brokerrpc.Quotes{
		Encoding: brokerrpc.Quotes_KAMBALA,
		Quotes:   quotes,
	})
	if err != nil {
		return err
	}

	cc.broker.sendCounter.Add(context.Background(), int64(len(quotes)))
	return nil
}

func (cc *clientConn) updateSubscriptions(req *brokerrpc.StreamRequest) {
	for _, sub := range req.Subscribe {
		cc.subs[int(sub)] = 0
	}

	for _, unsub := range req.Unsubscribe {
		delete(cc.subs, int(unsub))
	}
}
