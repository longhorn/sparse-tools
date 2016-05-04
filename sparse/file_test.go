package sparse

import (
	"sync"
	"testing"

	"github.com/rancher/sparse-tools/log"
)

func TestOrdering1(t *testing.T) {
	intervals := []Interval{
		{0, 1},
	}
    testOrdering(t, intervals)
}

func TestOrdering2(t *testing.T) {
	intervals := []Interval{
		{0, 1},
		{1, 2},
	}
    testOrdering(t, intervals)
}

func TestOrdering3(t *testing.T) {
	intervals := []Interval{
		{1, 2},
		{0, 1},
	}
    testOrdering(t, intervals)
}

func TestOrdering4(t *testing.T) {
	intervals := []Interval{
		{0, 1},
		{1, 2},
		{3, 4},
		{5, 6},
		{2, 3},
		{4, 5},
	}
    testOrdering(t, intervals)
}

func testOrdering(t *testing.T, intervals []Interval) {
	// Only log errors
	log.LevelPush(log.LevelError)
	defer log.LevelPop()

	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)

	for _, i := range intervals {       
		interval := Interval{i.Begin * Blocks, i.End * Blocks}
		hdi := HashedDataInterval{HashedInterval{FileInterval{SparseHole, interval}, make([]byte, 0)}, make([]byte, 0)}
		log.Debug("in:   ", hdi)
		unorderedStream <- hdi
	}
	close(unorderedStream)
	checkOrderedStream(t, "test  ", unorderedStream, orderedStream)    
}

func TestRandomOrdering(t *testing.T) {
	// Only log errors
	log.LevelPush(log.LevelError)
	defer log.LevelPop()

	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)
	size := 1000

	// Generate test data
	var wgroup sync.WaitGroup
	wgroup.Add(2)
	go func() {
		for i := 0; i < size; i += 2 {
			interval := Interval{int64(i) * Blocks, int64(i+1) * Blocks}
			hdi := HashedDataInterval{HashedInterval{FileInterval{SparseHole, interval}, make([]byte, 0)}, make([]byte, 0)}
			log.Debug("left: ", hdi)
			unorderedStream <- hdi
		}
		wgroup.Done()
	}()
	go func() {
		for i := 1; i < size; i += 2 {
			interval := Interval{int64(i) * Blocks, int64(i+1) * Blocks}
			hdi := HashedDataInterval{HashedInterval{FileInterval{SparseHole, interval}, make([]byte, 0)}, make([]byte, 0)}
			log.Debug("right:", hdi)
			unorderedStream <- hdi
		}
		wgroup.Done()
		// Finish data generation
		wgroup.Wait()
		close(unorderedStream)
	}()

    checkOrderedStream(t, "test  ", unorderedStream, orderedStream)
}

func checkOrderedStream(t *testing.T, prefix string, unorderedStream <-chan HashedDataInterval, orderedStream chan HashedDataInterval) {
	// Start ordering
	go OrderIntervals(prefix, unorderedStream, orderedStream)

	// check results
	pos := int64(0)
	for interval := range orderedStream {
		if pos != interval.Begin {
			t.Fatal("interval order violation", interval, "@", pos/Blocks)
		}
		pos = interval.End
	}    
}