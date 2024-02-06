package processor

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/aarityatech/pkg/instrumentlut"
	"github.com/aarityatech/pkg/log"

	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
)

type futureCalculator struct {
	lut          LUT
	clock        func() time.Time
	cache        quoteCache
	underlyings  []int
	expiryGroups map[groupID]*expiryGroup
}

// groupID is a tuple of underlying and expiry (as seconds since epoch).
type groupID [2]int

type expiryGroup struct {
	Expiry       int
	Underlying   int
	Options      []optionEntry
	Future       int
	ClosestOpt   *atomic.Int64 // position of closest strike option. -1 if future.
	LastComputed *atomic.Int64
}

type optionEntry struct {
	Strike int64
	CallID int
	PutID  int
}

func newFutureCalculator(cache quoteCache, lut LUT, clock func() time.Time) *futureCalculator {
	futCalc := &futureCalculator{
		lut:          lut,
		clock:        clock,
		cache:        cache,
		expiryGroups: map[groupID]*expiryGroup{},
	}

	for _, id := range lut.AllIDs() {
		instr, _ := lut.ByID(id)
		if len(instr.Derivatives) == 0 {
			// has no derivatives. skip.
			continue
		}

		m := futCalc.prepareExpiryGroup(instr, lut)
		for idTuple, de := range m {
			futCalc.expiryGroups[idTuple] = de
		}

		futCalc.underlyings = append(futCalc.underlyings, id)
	}

	futCalc.computeClosest()
	return futCalc
}

func (futCalc *futureCalculator) compute(quote receiver.Quote) []receiver.Quote {
	result := []receiver.Quote{quote}

	instr, ok := futCalc.lut.ByID(quote.ID)
	if !ok || !instr.IsFnO() {
		// We only re-compute for FnO instruments. For index and equity, we just
		// return the quote as-is.
		return result
	}

	gid := groupID{instr.UnderlyingID, getExpiryTime(instr.Expiry)}
	group, ok := futCalc.expiryGroups[gid]
	if !ok {
		// This should never happen. We should have already prepared the expiry
		// group for this instrument.
		log.Warn(context.Background(), "expiry group not found",
			log.Int("id", instr.ID),
			log.Time("expiry", instr.Expiry),
			log.Int("underlying_id", instr.UnderlyingID))
		return result
	}

	// The following block computes the synthetic future price if and only if
	// the quote would affect the future price. If the quote is for an instrument
	// that would not affect the future-price for the given expiry-group, we will
	// skip this computation and return the last computed value.
	var futurePrice int64
	if group.Future != 0 {
		if instr.ID == group.Future {
			futurePrice = quote.LTP()
		}
	} else {
		idx := group.ClosestOpt.Load()
		if idx == -1 {
			// We don't have any closest option yet. Return the quote as-is.
			return result
		}

		var callLTP, putLTP int64
		opt := group.Options[idx]
		if instr.ID == opt.CallID {
			quotes := futCalc.cache.Get([]int{opt.PutID})
			if len(quotes) == 1 {
				callLTP = quote.LTP()
				putLTP = quotes[0].LTP()

				futurePrice = callLTP - putLTP + opt.Strike
			}
		} else if instr.ID == opt.PutID {
			quotes := futCalc.cache.Get([]int{opt.CallID})
			if len(quotes) == 1 {
				callLTP = quotes[0].LTP()
				putLTP = quote.LTP()

				futurePrice = callLTP - putLTP + opt.Strike
			}
		}
	}

	if futurePrice == 0 {
		// We don't have a valid future price yet. Return last computed value.
		oldQuote := futCalc.cache.Get([]int{instr.ID})
		if len(oldQuote) > 0 && oldQuote[0].DerFut != 0 {
			result[0].DerFut = oldQuote[0].DerFut
		} else {
			result[0].DerFut = group.LastComputed.Load()
		}
		return result
	}

	// Store the newly computed value for later use.
	group.LastComputed.Store(futurePrice)

	// Synthetic future price is computed. Now, we need to update all quotes
	// that are affected by this.
	var affectedIDs []int
	for _, optEntry := range group.Options {
		if optEntry.CallID != quote.ID {
			affectedIDs = append(affectedIDs, optEntry.CallID)
		}

		if optEntry.PutID != quote.ID {
			affectedIDs = append(affectedIDs, optEntry.PutID)
		}
	}

	now := futCalc.clock()
	quotes := futCalc.cache.Get(affectedIDs)
	for i := range quotes {
		if quotes[i].DerFut != futurePrice {
			quotes[i].DerFut = futurePrice
			quotes[i].Time = now
			result = append(result, quotes[i])
		}
	}

	return result
}

func (futCalc *futureCalculator) prepareExpiryGroup(instr instrumentlut.Instrument, lut LUT) map[groupID]*expiryGroup {
	groups := map[groupID]*expiryGroup{}

	now := futCalc.clock()
	today := getBOD(now)

	for _, id := range instr.Derivatives {
		derivative, _ := lut.ByID(id)
		if derivative.Expiry.Before(today) {
			continue
		}

		expTime := getExpiryTime(derivative.Expiry)
		expID := [2]int{instr.ID, expTime}

		group, ok := groups[expID]
		if !ok {
			v := &atomic.Int64{}
			v.Store(-1)
			group = &expiryGroup{
				Expiry:       expTime,
				Underlying:   instr.ID,
				ClosestOpt:   v,
				LastComputed: &atomic.Int64{},
			}
		}

		strikePrice := int64(derivative.StrikePrice * 100)
		if derivative.Type == instrumentlut.InstrumentTypeFUT {
			group.Future = id
		} else {
			idx := -1

			for i := range group.Options {
				if group.Options[i].Strike == strikePrice {
					idx = i
					break
				}
			}

			if idx == -1 {
				group.Options = append(group.Options, optionEntry{})
				idx = len(group.Options) - 1
			}

			group.Options[idx].Strike = strikePrice

			if derivative.Type == instrumentlut.InstrumentTypeOPTCE {
				group.Options[idx].CallID = id
			} else {
				group.Options[idx].PutID = id
			}
		}

		sort.Slice(group.Options, func(i, j int) bool {
			return group.Options[i].Strike < group.Options[j].Strike
		})

		groups[expID] = group
	}

	return groups
}

func (futCalc *futureCalculator) computeClosest() {
	quotes := futCalc.cache.Get(futCalc.underlyings)
	ltps := map[int]int64{}
	for _, quote := range quotes {
		ltps[quote.ID] = quote.LTP()
	}

	for _, de := range futCalc.expiryGroups {
		if de.Future == 0 {
			underlyingLTP := ltps[de.Underlying]
			entryPos := findClosest(de.Options, underlyingLTP)
			de.ClosestOpt.Store(int64(entryPos))
		} else {
			futQuotes := futCalc.cache.Get([]int{de.Future})
			if len(futQuotes) == 1 {
				de.LastComputed.Store(futQuotes[0].LTP())
			}
		}
	}
}

func findClosest(arr []optionEntry, target int64) int {
	n := len(arr)
	if len(arr) == 0 {
		return -1
	} else if len(arr) == 1 {
		return 0
	}

	if target <= arr[0].Strike {
		return 0
	}
	if target >= arr[n-1].Strike {
		return n - 1
	}

	i, j, mid := 0, n, 0
	closest := 0
	for i < j {
		mid = i + (j-i)/2
		if arr[mid].Strike == target {
			return mid
		}

		// Update closest if arr[mid] is closer to target
		if math.Abs(float64(target-arr[mid].Strike)) < math.Abs(float64(target-arr[closest].Strike)) {
			closest = mid
		}

		if target < arr[mid].Strike {
			j = mid
		} else {
			i = mid + 1
		}
	}
	return closest
}

func getExpiryTime(t time.Time) int {
	return int(getBOD(t).Unix())
}

func getBOD(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
