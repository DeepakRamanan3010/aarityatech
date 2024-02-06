package precache_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aarityatech/tickerfeed/internal/feedbroker/receiver"
	"github.com/aarityatech/tickerfeed/pkg/precache"
)

var res []receiver.Quote

func BenchmarkAtomicCache_Get(b *testing.B) {
	keys := make([]int, 120_000)
	for i := 0; i < len(keys); i++ {
		keys[i] = i
	}
	ac := precache.New[receiver.Quote](keys)
	require.NotNil(b, ac)

	for i := 0; i < len(keys); i++ {
		ac.Put(i, receiver.Quote{ID: i})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res = ac.Get([]int{10_000, 5, 10})
	}
	fmt.Println(len(res))
}

func BenchmarkAtomicCache_Put(b *testing.B) {
	ac := precache.New[string]([]int{1, 2})
	require.NotNil(b, ac)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ac.Put(1, "hello")
	}
}
