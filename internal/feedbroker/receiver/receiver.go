package receiver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/aarityatech/pkg/log"
	"github.com/aarityatech/pkg/udprecv"
	"go.opentelemetry.io/otel"
)

var meter = otel.GetMeterProvider().Meter("feedbroker/receiver")

var (
	errTooShort          = errors.New("short packet")
	errInvalidToken      = errors.New("invalid token")
	errUnknownInstrument = errors.New("unknown instrument")
)

// Cache acts as a place to push the received packets.
type Cache interface {
	Get(keys []int) []Quote
	Put(key int, quote Quote)
}

// Processor is responsible for any quote pre-processing before it is
// sent to the cache.
type Processor interface {
	// Process is called for each quote received. Processor can return
	// multiples quotes if the incoming quote causes a change in the state
	// of other instruments. Processor must return the original quote also
	// if it is to be cached.
	Process(quote Quote) []Quote
}

// Quote is a single quote packet.
type Quote struct {
	ID     int       `json:"id"`
	Time   time.Time `json:"time"`
	Data   []byte    `json:"data"`
	DerFut int64     `json:"der_fut"`
}

// LTP returns the last traded price in the quote.
func (q Quote) LTP() int64 {
	return int64(binary.LittleEndian.Uint32(q.Data[39:43]))
}

// LUT provides a mapping between exchange segment and token to
// instrument ID.
type LUT interface {
	IDByToken(exchSegment string, exchToken int) (int, bool)
}

// Receiver provides facilities to continuously read UDP packets, parseQuote,
// pre-process and distribute.
type Receiver struct {
	cfg         Config
	lut         LUT
	cache       Cache
	diskW       *diskWriter
	rawRecv     *udprecv.UDPReceiver
	processor   Processor
	workerChans []chan Quote
}

// New initialises a new UDP receiver instance.
func New(cfg Config, lut LUT, cache Cache, processor Processor) (*Receiver, error) {
	if err := cfg.sanitise(); err != nil {
		return nil, err
	}

	var quotes []Quote
	var diskW *diskWriter
	if cfg.DiskCache.Enable {
		dc, err := openCache(cfg.DiskCache.CachePath, cfg.DiskCache.FlushInterval)
		if err != nil {
			return nil, err
		}
		diskW = dc

		quotes, err = diskW.GetAll(context.Background())
		if err != nil {
			return nil, err
		}
		for i := range quotes {
			cache.Put(quotes[i].ID, quotes[i])
		}
	}

	udpRec, err := udprecv.New(cfg.Config)
	if err != nil {
		return nil, err
	}

	workerChans := make([]chan Quote, cfg.Workers)
	for i := range workerChans {
		workerChans[i] = make(chan Quote, 4096)
	}

	return &Receiver{
		cfg:         cfg,
		lut:         lut,
		diskW:       diskW,
		cache:       cache,
		rawRecv:     udpRec,
		processor:   processor,
		workerChans: workerChans,
	}, nil
}

// Get returns the quotes for the given IDs. If the quote is not found,
// it is skipped. So the returned slice may be smaller than the input.
func (recv *Receiver) Get(ids []int) []Quote {
	return recv.cache.Get(ids)
}

// Run starts the receiver. It blocks until the context is cancelled or
// a critical error occurs.
func (recv *Receiver) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if recv.diskW != nil {
		go func() {
			recv.diskW.RunFlusher(ctx)
			cancel() // we want the receiver to stop on disk flusher exit.
		}()
	}

	// start the workers.
	for i := range recv.workerChans {
		go recv.worker(ctx, recv.workerChans[i])
	}

	// start the receiver.
	return recv.rawRecv.Run(ctx, func(ctx context.Context, b []byte, recvAt time.Time) {
		q, err := recv.parseQuote(ctx, b, recvAt)
		if err != nil {
			log.Error(ctx, "handle packet", log.Err(err))
			return
		}

		workerID := recv.getWorkerIdx(q.ID)
		select {
		case <-ctx.Done():
		case recv.workerChans[workerID] <- q:
		}
	})
}

func (recv *Receiver) parseQuote(_ context.Context, pkt []byte, recvAt time.Time) (Quote, error) {
	const exchangePlusTokenSize = 36
	if len(pkt) < exchangePlusTokenSize {
		// not enough data to even extract identifier.
		return Quote{}, errTooShort
	}

	exchSegment := string(bytes.TrimRight(pkt[0:10], "\x00"))
	exchTokenStr := string(bytes.TrimRight(pkt[10:35], "\x00"))

	exchToken, err := strconv.Atoi(exchTokenStr)
	if err != nil {
		// invalid token.
		return Quote{}, fmt.Errorf("%w: %v", errInvalidToken, err)
	}

	instrID, found := recv.lut.IDByToken(exchSegment, exchToken)
	if !found {
		return Quote{}, fmt.Errorf("%w: %s-%d", errUnknownInstrument, exchSegment, exchToken)
	}

	// clone it since the pkt is sliced from shared buffer.
	clone := make([]byte, len(pkt))
	copy(clone, pkt)

	return Quote{
		ID:   instrID,
		Time: recvAt,
		Data: clone,
	}, nil
}

func (recv *Receiver) worker(ctx context.Context, quotes chan Quote) {
	for {
		select {
		case <-ctx.Done():
			return

		case q := <-quotes:
			result := recv.processor.Process(q)
			for _, quote := range result {
				// Put it to in-memory cache.
				recv.cache.Put(quote.ID, quote)

				// send to disk-cache-writer if enabled.
				if recv.diskW != nil {
					recv.diskW.Enqueue(quote)
				}
			}
		}
	}
}

func (recv *Receiver) getWorkerIdx(key int) uint64 {
	// fast path for single worker.
	if recv.cfg.Workers == 1 {
		return 0
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(key))
	hasher := fnv.New64a()
	_, err := hasher.Write(buf[:])
	if err != nil {
		panic(err) // should never happen
	}
	return hasher.Sum64() % uint64(recv.cfg.Workers)
}
