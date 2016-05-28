package stats

import (
	"io/ioutil"
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

func appendSample(sample dataPoint) {
	samples <- sample
}

func verifySampleDurations(model []int) bool {
	for _, expected := range model {
		sample := <-samples
		if sample.duration != time.Duration(expected) {
			log.Error("queue mismatch; expected=", sample.duration, "actual=", time.Duration(expected))
			return false
		}
	}
	return true
}

func Test1(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), 0, OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]int{1000, 2000}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test2(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), 0, OpRead, 1024)

	<-Print()
}

func Test3(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(5000), 0, OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]int{2000, 3000, 4000, 5000}) {
		t.Fatal("sample mismatch:", samples)
	}
}

func Test4(t *testing.T) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()
	resetStats(4)

	Sample(time.Now(), time.Duration(1000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(2000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(3000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(4000), 0, OpRead, 1024)
	Sample(time.Now(), time.Duration(5000), 0, OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]int{2000, 3000, 4000, 5000}) {
		t.Fatal("sample mismatch:", samples)
	}

	Sample(time.Now(), time.Duration(6000), 0, OpRead, 1024)

	<-Process(appendSample)
	if !verifySampleDurations([]int{6000}) {
		t.Fatal("sample mismatch:", samples)
	}
}
