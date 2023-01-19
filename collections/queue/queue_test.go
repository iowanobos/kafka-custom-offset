package queue

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	randomValuesSize := 10
	randomValues := make([]int64, randomValuesSize)
	randomExpected := make([]int64, randomValuesSize)
	for i, value := range rand.New(rand.NewSource(time.Now().UnixNano())).Perm(randomValuesSize) {
		randomValues[i] = int64(value)
		randomExpected[i] = int64(i)
	}

	tests := map[string]struct {
		values   []int64
		expected []int64
	}{
		"ascending ordered input":  {values: []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, expected: []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		"descending ordered input": {values: []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9}, expected: []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		"repeated input values":    {values: []int64{7, 3, 9, 1, 2, 0, -3, 3, 4, 2, 6, 6}, expected: []int64{-3, 0, 1, 2, 2, 3, 3, 4, 6, 6, 7, 9}},
		"all random":               {values: randomValues, expected: randomExpected},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			q := New()
			for _, value := range tc.values {
				q.Push(value)
			}

			l := q.Len()
			actual := make([]int64, l)
			for i := 0; i < l; i++ {
				actual[i] = q.Pop()
			}

			assert.Equal(t, tc.expected, actual)
		})
	}
}
