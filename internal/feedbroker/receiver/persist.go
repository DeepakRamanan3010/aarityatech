package receiver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/aarityatech/pkg/log"
	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func openCache(dir string, interval time.Duration) (*diskWriter, error) {
	opts := badger.DefaultOptions(dir)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	dc := &diskWriter{
		db:       db,
		buf:      make(chan Quote, 65536),
		done:     make(chan struct{}),
		interval: interval,
	}
	dc.createMeasures()

	return dc, nil
}

type diskWriter struct {
	db       *badger.DB
	buf      chan Quote
	done     chan struct{}
	interval time.Duration

	// open-telemetry metrics
	flushDuration metric.Int64Histogram
}

func (dc *diskWriter) Enqueue(q Quote) {
	select {
	case dc.buf <- q:
		// successful enqueue.

	case <-dc.done:
		// cache is closed.
	}
}

func (dc *diskWriter) GetAll(ctx context.Context) ([]Quote, error) {
	var quotes []Quote
	err := dc.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var quote Quote
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			data, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			if err := json.Unmarshal(data, &quote); err != nil {
				log.Warn(ctx, "failed to unmarshal quote", log.Err(err))
				continue
			}

			quotes = append(quotes, quote)
		}

		return nil
	})
	return quotes, err
}

func (dc *diskWriter) RunFlusher(ctx context.Context) {
	defer close(dc.done)

	pos := 0
	buf := make([]Quote, 4096)

	tic := time.NewTicker(dc.interval)
	defer tic.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case q := <-dc.buf:
			if pos >= len(buf) {
				if err := dc.flush(buf); err != nil {
					log.Warn(ctx, "failed to flush to disk", log.Err(err))
				}
				pos = 0
			}

			buf[pos] = q
			pos++

		case <-tic.C:
			if pos == 0 {
				continue
			}

			if err := dc.flush(buf[:pos]); err != nil {
				log.Warn(ctx, "failed to flush to disk", log.Err(err))
			}
			pos = 0
		}
	}
}

func (dc *diskWriter) Close() error { return dc.db.Close() }

func (dc *diskWriter) flush(buf []Quote) error {
	t := time.Now()
	defer func() {
		dc.flushDuration.Record(context.Background(), time.Since(t).Milliseconds())
	}()

	b := dc.db.NewWriteBatch()

	var idBytes [8]byte
	for _, q := range buf {
		binary.LittleEndian.PutUint64(idBytes[:], uint64(q.ID))

		data, err := json.Marshal(q)
		if err != nil {
			b.Cancel()
			return err
		}

		if err := b.Set(idBytes[:], data); err != nil {
			b.Cancel()
			return err
		}
	}

	return b.Flush()
}

func (dc *diskWriter) createMeasures() {
	var err error
	dc.flushDuration, err = meter.Int64Histogram("cache.flush.duration", metric.WithUnit("ms"))
	if err != nil {
		otel.Handle(err)
	}
}
