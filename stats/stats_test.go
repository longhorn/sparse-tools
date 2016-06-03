package stats

import (
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/rancher/sparse-tools/log"
)

func tempFilePath() string {
	// Make a temporary file path
	f, err := ioutil.TempFile("", "stat-test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

var (
	samples = make(chan dataPoint, 4)
)

func resetSamples() {
	close(samples)
	samples = make(chan dataPoint, 4)
}

func appendSample(sample dataPoint) {
	samples <- sample
}

type model struct {
	duration, size int
	durationIgnore bool
}

// drops veryfied samples
func verifySampleDurations(m []model) bool {
	for _, expected := range m {
		sample := <-samples
		if !expected.durationIgnore && sample.duration != time.Duration(expected.duration) {
			log.Error("queue duration mismatch; expected=", sample.duration, "actual=", time.Duration(expected.duration))
			return false
		}
		if sample.size != expected.size {
			log.Error("queue size mismatch; expected=", sample.size, "actual=", expected.size)
			return false
		}
	}
	return true
}

func Test1(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "", OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]model{{1000, 1024, false}, {2000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test2(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "replica-a", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "replica-b", OpRead, 1024)

	<-Print()
}

func Test3(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(5000), "", OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]model{{2000, 1024, false}, {3000, 1024, false}, {4000, 1024, false}, {5000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test4(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(5000), "", OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]model{{2000, 1024, false}, {3000, 1024, false}, {4000, 1024, false}, {5000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}

	Sample(time.Now(), time.Duration(6000), "", OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]model{{6000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test5(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	var pending []OpID
	pending = append(pending, InsertPendingOp(time.Now(), "", OpRead, 2048))
	pending = append(pending, InsertPendingOp(time.Now(), "", OpWrite, 4096))
	err := RemovePendingOp(pending[0])
	if err != nil {
		t.Fatal(err)
	}

	<-Print()

	err = RemovePendingOp(pending[1])
	if err != nil {
		t.Fatal(err)
	}
}

func Test6(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	var pending []OpID
	pending = append(pending, InsertPendingOp(time.Now(), "", OpRead, 2048))
	pending = append(pending, InsertPendingOp(time.Now(), "", OpWrite, 4096))
	err := RemovePendingOp(pending[0])
	if err != nil {
		t.Fatal(err)
	}
	pending = append(pending, InsertPendingOp(time.Now(), "", OpPing, 8192))

	<-Process(appendSample) // 1kB, 2kB
	if !verifySampleDurations([]model{{1000, 1024, false}, {0, 2048, true}}) {
		t.Fatal("sample mismatch:", samples)
	}

	err = RemovePendingOp(pending[1])
	if err != nil {
		t.Fatal(err)
	}
	err = RemovePendingOp(pending[2])
	if err != nil {
		t.Fatal(err)
	}

	<-Process(appendSample) // 4kB, 8kB
	if !verifySampleDurations([]model{{0, 4096, false}, {0, 8192, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test7(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	var pending []OpID
	pending = append(pending, InsertPendingOp(time.Now(), "", OpRead, 2048))
	pending = append(pending, InsertPendingOp(time.Now(), "", OpWrite, 4096))

	err := RemovePendingOp(pending[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingOpsFreeSlot) != 1 {
		t.Fatal("pendingOpsFreeSlot stack failure", pendingOpsFreeSlot)
	}

	pending = append(pending, InsertPendingOp(time.Now(), "", OpPing, 8192))
	if len(pendingOpsFreeSlot) != 0 {
		t.Fatal("pendingOpsFreeSlot stack failure", pendingOpsFreeSlot)
	}

	err = RemovePendingOp(pending[1])
	if err != nil {
		t.Fatal(err)
	}
	err = RemovePendingOp(pending[2])
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingOpsFreeSlot) != 2 {
		t.Fatal("pendingOpsFreeSlot stack failure", pendingOpsFreeSlot)
	}

	resetSamples() // cleanup for other tests
}

// test ProcessLimited
func Test8(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), "", OpRead, 1024)

	<-ProcessLimited(2, appendSample)
	if !verifySampleDurations([]model{{3000, 1024, false}, {4000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

// test ProcessLimited; unspecified limit
func Test9(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), "", OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), "", OpRead, 1024)

	<-ProcessLimited(0, appendSample)
	if !verifySampleDurations([]model{{1000, 1024, false}, {2000, 1024, false}, {3000, 1024, false}, {4000, 1024, false}}) {
		t.Fatal("sample mismatch:", samples)
	}
}

// test targetID
func Test10(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), "a", OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), "b", OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), "a", OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), "b", OpRead, 1024)

	<-Process(appendSample)
	sample := <-samples
	if targetID(sample.target) != "a" {
		t.Fatal("sample target mismatch:", samples)
	}
	sample = <-samples
	if targetID(sample.target) != "b" {
		t.Fatal("sample target mismatch:", samples)
	}
	sample = <-samples
	if targetID(sample.target) != "a" {
		t.Fatal("sample target mismatch:", samples)
	}
	sample = <-samples
	if targetID(sample.target) != "b" {
		t.Fatal("sample target mismatch:", samples)
	}
}

func benchmark(store func(sample dataPoint), fibers int, b *testing.B) {
	var sample dataPoint
	wg := &sync.WaitGroup{}
	for i := 0; i < fibers; i++ {
		wg.Add(1)
		go func() {
			for run := 0; run < b.N; run++ {
				store(sample)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func Benchmark_C1(b *testing.B) {
	benchmark(storeSample, 1, b)
}
func Benchmark_C10(b *testing.B) {
	benchmark(storeSample, 10, b)
}
func Benchmark_C100(b *testing.B) {
	benchmark(storeSample, 100, b)
}
