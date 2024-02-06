package processor

import (
	"testing"
	"time"

	"github.com/aarityatech/pkg/instrumentlut"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
)

func Test_newFutureCalculator(t *testing.T) {
	t.Parallel()

	cache := fakeCache(func(keys []int) []receiver.Quote {
		return nil
	})

	lut := fakeLUT{
		{ID: 1, Derivatives: []int{2}},
		{ID: 2, Expiry: time.Date(2024, 1, 20, 0, 0, 0, 0, time.Local)},
	}

	calc := newFutureCalculator(cache, lut, func() time.Time {
		return time.Date(2024, 1, 20, 0, 0, 0, 0, time.Local)
	})
	require.NotNil(t, calc)

	assert.Equal(t, []int{1}, calc.underlyings)
	assert.Equal(t, 1, len(calc.expiryGroups))
}

func TestFutureCalculator_compute(t *testing.T) {
	t.Parallel()

	cache := fakeCache(func(keys []int) []receiver.Quote {
		return nil
	})

	lut := fakeLUT{
		{
			ID:          1,
			Type:        instrumentlut.InstrumentTypeIDX,
			Derivatives: []int{2, 3},
		},
		{
			ID:           2,
			UnderlyingID: 1,
			Type:         instrumentlut.InstrumentTypeOPTCE,
			Expiry:       time.Date(2024, 1, 20, 0, 0, 0, 0, time.Local),
		},
		{
			ID:           3,
			UnderlyingID: 1,
			Type:         instrumentlut.InstrumentTypeOPTCE,
			Expiry:       time.Date(2024, 1, 30, 0, 0, 0, 0, time.Local),
		},
	}

	tests := []struct {
		title string
		quote receiver.Quote
		want  []receiver.Quote
	}{
		{
			title: "UnderlyingQuote",
			quote: receiver.Quote{ID: 1},
			want:  []receiver.Quote{{ID: 1}},
		},
		{
			title: "DerivativeQuote",
			quote: receiver.Quote{ID: 2},
			want:  []receiver.Quote{{ID: 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			calc := newFutureCalculator(cache, lut, func() time.Time {
				return time.Date(2024, 1, 20, 0, 0, 0, 0, time.Local)
			})
			require.NotNil(t, calc)

			result := calc.compute(tt.quote)
			assert.Equal(t, tt.want, result)
		})
	}

}

func Test_findClosest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		title  string
		arr    []optionEntry
		target int64
		want   int
	}{
		{
			title:  "Empty",
			arr:    nil,
			target: 200,
			want:   -1,
		},
		{
			title: "Single",
			arr: []optionEntry{
				{100, 1, 2},
			},
			target: 200,
			want:   0,
		},

		{
			title: "SmallerThanFirst",
			arr: []optionEntry{
				{100, 1, 2},
				{200, 3, 4},
				{300, 5, 6},
			},
			target: 10,
			want:   0,
		},
		{
			title: "ExactMid",
			arr: []optionEntry{
				{100, 1, 2},
				{200, 3, 4},
				{300, 5, 6},
			},
			target: 200,
			want:   1,
		},
		{
			title: "LastItem",
			arr: []optionEntry{
				{100, 1, 2},
				{200, 3, 4},
				{300, 5, 6},
			},
			target: 300,
			want:   2,
		},

		{
			title: "ClosestOnly",
			arr: []optionEntry{
				{100, 1, 2},
				{200, 3, 4},
				{300, 5, 6},
				{700, 5, 6},
			},
			target: 400,
			want:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.title, func(t *testing.T) {
			got := findClosest(tt.arr, tt.target)
			assert.Equal(t, tt.want, got)
		})
	}
}

type fakeCache func(keys []int) []receiver.Quote

func (f fakeCache) Get(keys []int) []receiver.Quote {
	return f(keys)
}

type fakeLUT []instrumentlut.Instrument

func (f fakeLUT) ByID(id int) (instrumentlut.Instrument, bool) {
	for _, instrument := range f {
		if instrument.ID == id {
			return instrument, true
		}
	}
	return instrumentlut.Instrument{}, false
}

func (f fakeLUT) AllIDs() []int {
	ids := make([]int, len(f))
	for i, instrument := range f {
		ids[i] = instrument.ID
	}
	return ids
}
