package feedserver

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/aarityatech/pkg/marketfeed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_messageEnvelope(t *testing.T) {
	t.Run("NoOp", func(t *testing.T) {
		count := 0
		me := allocMessageEnvelope(func(buf []byte) {
			count++
		})
		me.flush()

		assert.Equal(t, 0, count)
	})

	t.Run("SingleWrite", func(t *testing.T) {
		me := allocMessageEnvelope(func(buf []byte) {
			count := int(binary.LittleEndian.Uint16(buf))
			assert.Equal(t, 1, count)

			pktType := int(binary.LittleEndian.Uint16(buf[2:4]))
			assert.Equal(t, 1, pktType)

			id := binary.LittleEndian.Uint64(buf[4:12])
			assert.Equal(t, uint64(122), id)
		})

		pkt := marketfeed.Packet{
			InstrumentID:    122,
			ExchangeSegment: "NSE",
			SymbolOrToken:   "22",
			LastTradePrice:  100,
		}
		me.addPacket(pkt, 1)
		me.flush()
	})

	t.Run("MultipleWrite", func(t *testing.T) {
		me := allocMessageEnvelope(func(buf []byte) {
			count := int(binary.LittleEndian.Uint16(buf[0:2]))
			assert.Equal(t, 2, count)

			pktType := int(binary.LittleEndian.Uint16(buf[2:4]))
			id := binary.LittleEndian.Uint64(buf[4:12])
			ltp := binary.LittleEndian.Uint32(buf[12:16])
			assert.Equal(t, 1, pktType, "wrong packet type")
			assert.Equal(t, uint64(123), id, "wrong instrument id")
			assert.Equal(t, uint32(100), ltp, "wrong LTP")

			pktType = int(binary.LittleEndian.Uint16(buf[16:18]))
			id = binary.LittleEndian.Uint64(buf[18:26])
			ltp = binary.LittleEndian.Uint32(buf[26:30])
			assert.Equal(t, 1, pktType, "wrong packet type")
			assert.Equal(t, uint64(124), id, "wrong instrument id")
			assert.Equal(t, uint32(200), ltp, "wrong LTP")
		})

		me.addPacket(marketfeed.Packet{InstrumentID: 123, LastTradePrice: 100}, 1)
		me.addPacket(marketfeed.Packet{InstrumentID: 124, LastTradePrice: 200}, 1)
		me.flush()
	})
}

func Test_writePacket(t *testing.T) {
	pkt := marketfeed.Packet{
		InstrumentID:       123,
		ExchangeSegment:    "NSE",
		SymbolOrToken:      "22",
		LastTradePrice:     12003,
		LastTradeQuantity:  100,
		LastTradeTime:      1388558422,
		AvgTradePrice:      12345,
		TotalBuyQuantity:   123,
		TotalSellQuantity:  456,
		OpenPrice:          12001,
		HighPrice:          12300,
		LowPrice:           11900,
		ClosingPrice:       12050,
		VolumeToday:        987654321,
		LowerCircuitLimit:  9000,
		UpperCircuitLimit:  15000,
		OpenInterest:       34566,
		TotalOpenInterest:  123890,
		DerivedFuturePrice: 456789,
		BestBids: [5]marketfeed.MarketDepth{
			{Qty: 100, Price: 12007, Orders: 1},
			{Qty: 200, Price: 12006, Orders: 2},
			{Qty: 300, Price: 12005, Orders: 3},
			{Qty: 400, Price: 12004, Orders: 4},
			{Qty: 500, Price: 12003, Orders: 5},
		},
		BestOffers: [5]marketfeed.MarketDepth{
			{Qty: 100, Price: 12008, Orders: 1},
			{Qty: 200, Price: 12009, Orders: 2},
			{Qty: 300, Price: 12010, Orders: 3},
			{Qty: 400, Price: 12011, Orders: 4},
			{Qty: 500, Price: 12012, Orders: 5},
		},
	}

	assertRestZero := func(buf []byte) {
		for i := 100; i < len(buf); i++ {
			assert.Zero(t, buf[i])
		}
	}

	t.Run("modeLTP", func(t *testing.T) {
		buf := make([]byte, 512)
		writePacket(buf, pkt, 1)

		assert.Equal(t, uint64(123), binary.LittleEndian.Uint64(buf[0:8]))
		assert.Equal(t, uint32(12003), binary.LittleEndian.Uint32(buf[8:12]))

		assertRestZero(buf[40:])
	})

	t.Run("modeQuote", func(t *testing.T) {
		buf := make([]byte, 512)
		writePacket(buf, pkt, 2)

		// ltp packet starts.
		assert.Equal(t, uint64(123), binary.LittleEndian.Uint64(buf[0:8]))
		assert.Equal(t, uint32(12003), binary.LittleEndian.Uint32(buf[8:12]))

		// quote packet starts.
		assert.Equal(t, uint32(100), binary.LittleEndian.Uint32(buf[12:16]))
		assert.Equal(t, uint64(1704091222), binary.LittleEndian.Uint64(buf[16:24]))
		assert.Equal(t, uint32(12345), binary.LittleEndian.Uint32(buf[24:28]))
		assert.Equal(t, uint32(12007), binary.LittleEndian.Uint32(buf[28:32]))
		assert.Equal(t, uint32(12008), binary.LittleEndian.Uint32(buf[32:36]))
		assert.Equal(t, uint32(12001), binary.LittleEndian.Uint32(buf[36:40]))
		assert.Equal(t, uint32(12300), binary.LittleEndian.Uint32(buf[40:44]))
		assert.Equal(t, uint32(11900), binary.LittleEndian.Uint32(buf[44:48]))
		assert.Equal(t, uint32(12050), binary.LittleEndian.Uint32(buf[48:52]))
		assert.Equal(t, uint32(987654321), binary.LittleEndian.Uint32(buf[52:56]))
		assert.Equal(t, uint32(9000), binary.LittleEndian.Uint32(buf[56:60]))
		assert.Equal(t, uint32(15000), binary.LittleEndian.Uint32(buf[60:64]))
		assert.Equal(t, uint32(34566), binary.LittleEndian.Uint32(buf[64:68]))
		assert.Equal(t, uint32(123), binary.LittleEndian.Uint32(buf[68:72]))
		assert.Equal(t, uint32(456), binary.LittleEndian.Uint32(buf[72:76]))

		assertRestZero(buf[100:])
	})

	t.Run("modeMarketDepth5", func(t *testing.T) {
		buf := make([]byte, 512)
		writePacket(buf, pkt, 3)

		// ltp packet starts.
		assert.Equal(t, uint64(123), binary.LittleEndian.Uint64(buf[0:8]))
		assert.Equal(t, uint32(12003), binary.LittleEndian.Uint32(buf[8:12]))

		// quote packet starts.
		assert.Equal(t, uint32(100), binary.LittleEndian.Uint32(buf[12:16]))
		assert.Equal(t, uint64(1704091222), binary.LittleEndian.Uint64(buf[16:24]))
		assert.Equal(t, uint32(12345), binary.LittleEndian.Uint32(buf[24:28]))
		assert.Equal(t, uint32(12007), binary.LittleEndian.Uint32(buf[28:32]))
		assert.Equal(t, uint32(12008), binary.LittleEndian.Uint32(buf[32:36]))
		assert.Equal(t, uint32(12001), binary.LittleEndian.Uint32(buf[36:40]))
		assert.Equal(t, uint32(12300), binary.LittleEndian.Uint32(buf[40:44]))
		assert.Equal(t, uint32(11900), binary.LittleEndian.Uint32(buf[44:48]))
		assert.Equal(t, uint32(12050), binary.LittleEndian.Uint32(buf[48:52]))
		assert.Equal(t, uint32(987654321), binary.LittleEndian.Uint32(buf[52:56]))
		assert.Equal(t, uint32(9000), binary.LittleEndian.Uint32(buf[56:60]))
		assert.Equal(t, uint32(15000), binary.LittleEndian.Uint32(buf[60:64]))
		assert.Equal(t, uint32(34566), binary.LittleEndian.Uint32(buf[64:68]))
		assert.Equal(t, uint32(123), binary.LittleEndian.Uint32(buf[68:72]))
		assert.Equal(t, uint32(456), binary.LittleEndian.Uint32(buf[72:76]))
		assert.Equal(t, uint32(456789), binary.LittleEndian.Uint32(buf[76:80]))

		// market depth packet starts.
		pos := 80
		for _, bid := range pkt.BestBids {
			assert.Equal(t, uint32(bid.Qty), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4

			assert.Equal(t, uint32(bid.Price), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4

			assert.Equal(t, uint32(bid.Orders), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4
		}

		for _, offer := range pkt.BestOffers {
			assert.Equal(t, uint32(offer.Qty), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4

			assert.Equal(t, uint32(offer.Price), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4

			assert.Equal(t, uint32(offer.Orders), binary.LittleEndian.Uint32(buf[pos:pos+4]))
			pos += 4
		}

		assertRestZero(buf[pos:])
	})

	t.Run("LastTradeTimeZero", func(t *testing.T) {
		pkt := pkt // copy
		pkt.LastTradeTime = 0
		pkt.SecondsSinceBOE = 1388558423

		buf := make([]byte, 512)
		writePacket(buf, pkt, 2)

		assert.Equal(t, int64(1704091223), int64(binary.LittleEndian.Uint64(buf[16:24])))
	})
}

func Test_modeSize(t *testing.T) {
	m := modeSize(1)
	assert.Equal(t, 12, m)

	m = modeSize(2)
	assert.Equal(t, 80, m)

	m = modeSize(3)
	assert.Equal(t, 200, m)

	m = modeSize(102) // invalid mode
	assert.Equal(t, 0, m)
}

func TestPacket_ReverseMap(t *testing.T) {
	const sample = `BAABAGYqAAAAAAAAOwMAAAEAEScAAAAAAABFAQAAAQASJwAAAAAAAA8DAAABABMnAAAAAAAA2AMAAA==`
	wantIDs := []int{10854, 10001, 10002, 10003}

	data, err := base64.StdEncoding.DecodeString(sample)
	require.NoError(t, err)

	count := int(binary.LittleEndian.Uint16(data[0:2]))
	assert.Equal(t, 4, count)

	offset := 2
	for i := 0; i < count; i++ {
		pktType := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		size := modeSize(pktType)
		assert.NotZero(t, size)

		id := int(binary.LittleEndian.Uint64(data[offset : offset+8]))
		assert.Equal(t, wantIDs[i], id)
		offset += size
	}
}
