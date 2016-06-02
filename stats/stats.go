package stats

import (
	"fmt"
	"time"

	"errors"

	"sync"

	"github.com/rancher/sparse-tools/log"
)

const (
	defaultBufferSize = 100 * 1000 // sample buffer size (cyclic)
)

//SampleOp operation
type SampleOp int

const (
	// OpNone unitialized operation
	OpNone SampleOp = iota
	// OpRead read from replica
	OpRead
	// OpWrite write to replica
	OpWrite
	// OpPing ping replica
	OpPing
)

type dataPoint struct {
	target    int // e.g replica index
	op        SampleOp
	timestamp time.Time
	duration  time.Duration
	size      int // i/o operation size
}

// String conversions
func (op SampleOp) String() string {
	switch op {
	case OpRead:
		return "R"
	case OpWrite:
		return "W"
	case OpPing:
		return "P"
	}
	return "<unknown op>"
}

func (sample dataPoint) String() string {
	if sample.duration != time.Duration(0) {
		return fmt.Sprintf("%s: #%d %v[%3dkB] %8dus", sample.timestamp.Format(time.StampMicro), sample.target, sample.op, sample.size/1024, sample.duration.Nanoseconds()/1000)
	}
	return fmt.Sprintf("%s: #%d %v[%3dkB] pending", sample.timestamp.Format(time.StampMicro), sample.target, sample.op, sample.size/1024)
}

var (
	bufferSize = defaultBufferSize
	cdata      = make(chan dataPoint, defaultBufferSize)
)

func initStats(size int) {
	bufferSize = size
	cdata = make(chan dataPoint, bufferSize)
	log.Debug("Stats.init=", size)
}

func init() {
	initStats(bufferSize)
}

func storeSample(sample dataPoint) {
	//Maintain non-blocking state
	for {
		select {
		case cdata <- sample:
			return
		default:
			<-cdata
		}
	}
}

// Sample to the cyclic buffer
func Sample(timestamp time.Time, duration time.Duration, target int, op SampleOp, size int) {
	storeSample(dataPoint{target, op, timestamp, duration, size})
}

// Process unreported samples
func Process(processor func(dataPoint)) chan struct{} {
	// Fetch unreported window
	done := make(chan struct{})
	go func(pending []dataPoint, done chan struct{}) {
	samples:
		for {
			select {
			case sample := <-cdata:
				log.Debug("Stats.Processing=", sample)
				processor(sample)
			default:
				break samples
			}
		}
		for _, sample := range pending {
			log.Debug("Stats.Processing pending=", sample)
			processor(sample)
		}
		close(done)
	}(getPendingOps(), done)
	return done
}

func printSample(sample dataPoint) {
	fmt.Println(sample)
}

// Print samples
func Print() chan struct{} {
	return Process(printSample)
}

// Test helper to exercise small buffer sizes
func resetStats(size int) {
	log.Debug("Stats.reset")
	initStats(size)
}

//OpID pending operation id
type OpID int

var (
	pendingOps      = make([]dataPoint, 8, 128)
	mutexPendingOps sync.Mutex
)

//InsertPendingOp starts tracking of a pending operation
func InsertPendingOp(timestamp time.Time, target int, op SampleOp, size int) OpID {
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	id := pendingOpEmptySlot()
	pendingOps[id] = dataPoint{target, op, timestamp, 0, size}
	log.Debug("InsertPendingOp id=", id)
	return OpID(id)
}

//RemovePendingOp removes tracking of a completed operation
func RemovePendingOp(id OpID) error {
	log.Debug("RemovePendingOp id=", id)
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	i := int(id)
	if i < 0 || i >= len(pendingOps) {
		errMsg := "RemovePendingOp: Invalid OpID"
		log.Error(errMsg, i)
		return errors.New(errMsg)
	}
	if pendingOps[i].op == OpNone {
		errMsg := "RemovePendingOp: OpID already removed"
		log.Error(errMsg, i)
		return errors.New(errMsg)
	}

	// Update the duration and store in the recent stats
	pendingOps[i].duration = time.Now().Sub(pendingOps[i].timestamp)
	storeSample(pendingOps[i])

	//Remove from pending
	pendingOps[i].op = OpNone
	return nil
}

func pendingOpEmptySlot() int {
	for i, op := range pendingOps {
		if op.op == OpNone {
			return i
		}
	}
	pendingOps = append(pendingOps, dataPoint{})
	return len(pendingOps) - 1
}

func getPendingOps() []dataPoint {
	mutexPendingOps.Lock()
	defer mutexPendingOps.Unlock()

	ops := make([]dataPoint, 0, len(pendingOps))
	for _, op := range pendingOps {
		if op.op != OpNone {
			ops = append(ops, op)
		}
	}
	return ops
}
