package receiver

import (
	"time"

	"github.com/aarityatech/pkg/marketfeed"
)

type lastQuote struct {
	id         int
	val        marketfeed.Packet
	hasVal     bool
	lastNotify time.Time
}

func (td *lastQuote) clone() *lastQuote {
	return &lastQuote{
		id:         td.id,
		val:        td.val,
		hasVal:     td.hasVal,
		lastNotify: td.lastNotify,
	}
}
