package feedserver

import (
	"encoding/binary"
	"time"

	"github.com/aarityatech/pkg/marketfeed"
)

const bufSize = 2048

const (
	modeUnsub        = 0
	modeLTP          = 1
	modeQuote        = 2
	modeMarketDepth5 = 3
)

var epoch = time.Date(1980, time.January, 1, 0, 0, 0, 0, time.UTC)

func allocMessageEnvelope(write func([]byte)) messageEnvelope {
	return messageEnvelope{
		buf:   make([]byte, bufSize),
		pos:   2, // leave space for the packet count.
		write: write,
	}
}

type messageEnvelope struct {
	buf   []byte
	pos   int // current position in the buffer
	count int // number of packets in the buffer
	write func(buf []byte)
}

func (m *messageEnvelope) addPacket(pkt marketfeed.Packet, mode int) {
	size := modeSize(mode)
	if size == 0 {
		return // invalid mode
	}

	// flush the buffer if there is not enough space.
	if len(m.buf[m.pos:]) < size {
		m.flush()
	}
	m.count++

	binary.LittleEndian.PutUint16(m.buf[m.pos:m.pos+2], uint16(mode))
	m.pos += 2

	writePacket(m.buf[m.pos:], pkt, mode)
	m.pos += size
}

func (m *messageEnvelope) flush() {
	if m.count == 0 {
		return // nothing to flush
	}

	// set the packet count and flush the buffer
	binary.LittleEndian.PutUint16(m.buf, uint16(m.count))
	m.write(m.buf[:m.pos])

	// reset the buffer
	m.count = 0
	m.pos = 2
}

func writePacket(buf []byte, pkt marketfeed.Packet, mode int) {
	binary.LittleEndian.PutUint64(buf[0:8], uint64(pkt.InstrumentID))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(pkt.LastTradePrice))

	if mode >= modeQuote {
		binary.LittleEndian.PutUint32(buf[12:16], uint32(pkt.LastTradeQuantity))
		if pkt.LastTradeTime <= 0 {
			binary.LittleEndian.PutUint64(buf[16:24], uint64(adjustToUnixEpoch(pkt.SecondsSinceBOE)))
		} else {
			binary.LittleEndian.PutUint64(buf[16:24], uint64(adjustToUnixEpoch(pkt.LastTradeTime)))
		}
		binary.LittleEndian.PutUint32(buf[24:28], uint32(pkt.AvgTradePrice))
		binary.LittleEndian.PutUint32(buf[28:32], uint32(pkt.BestBids[0].Price))
		binary.LittleEndian.PutUint32(buf[32:36], uint32(pkt.BestOffers[0].Price))
		binary.LittleEndian.PutUint32(buf[36:40], uint32(pkt.OpenPrice))
		binary.LittleEndian.PutUint32(buf[40:44], uint32(pkt.HighPrice))
		binary.LittleEndian.PutUint32(buf[44:48], uint32(pkt.LowPrice))
		binary.LittleEndian.PutUint32(buf[48:52], uint32(pkt.ClosingPrice))
		binary.LittleEndian.PutUint32(buf[52:56], uint32(pkt.VolumeToday))
		binary.LittleEndian.PutUint32(buf[56:60], uint32(pkt.LowerCircuitLimit))
		binary.LittleEndian.PutUint32(buf[60:64], uint32(pkt.UpperCircuitLimit))
		binary.LittleEndian.PutUint32(buf[64:68], uint32(pkt.OpenInterest))
		binary.LittleEndian.PutUint32(buf[68:72], uint32(pkt.TotalBuyQuantity))
		binary.LittleEndian.PutUint32(buf[72:76], uint32(pkt.TotalSellQuantity))
		binary.LittleEndian.PutUint32(buf[76:80], uint32(pkt.DerivedFuturePrice))
	}

	if mode >= modeMarketDepth5 {
		pos := 80
		for _, bid := range pkt.BestBids {
			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(bid.Qty))
			pos += 4

			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(bid.Price))
			pos += 4

			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(bid.Orders))
			pos += 4
		}

		for _, offer := range pkt.BestOffers {
			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(offer.Qty))
			pos += 4

			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(offer.Price))
			pos += 4

			binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(offer.Orders))
			pos += 4
		}
	}
}

func modeSize(mode int) int {
	switch mode {
	case modeLTP:
		return 12

	case modeQuote:
		// 12 bytes for LTP + 68 bytes for quote = 80 bytes
		return 80

	case modeMarketDepth5:
		// 80 bytes for quote + 5 bids & 5 offers = 200 bytes
		return 200
	}

	return 0
}

func adjustToUnixEpoch(t int64) int64 {
	if t <= 0 {
		return 0
	}
	return epoch.Add(time.Duration(t) * time.Second).Unix()
}
