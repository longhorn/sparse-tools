package sparse_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	. "github.com/rancher/sparse-tools/sparse"

	"github.com/rancher/sparse-tools/log"
)

const batch = 32 // blocks for read/write

type TestFileInterval struct {
	FileInterval
	dataMask byte // XORed with other generated data bytes
}

func (i TestFileInterval) String() string {
	kind := " "
	if i.Kind == SparseData {
		kind = "D"
	}
	return fmt.Sprintf("%s[%8d:%8d](%3d) %2X}", kind, i.Interval.Begin/Blocks, i.Interval.End/Blocks, i.Interval.Len()/Blocks, i.dataMask)
}

func TestRandomLayout10MB(t *testing.T) {
	const seed = 0
	const size = 10 /*MB*/ << 20
	prefix := "ssync"
	name := tempFileName(prefix)
	layoutStream := generateLayout(prefix, size, seed)
	layout1, layout2 := teeLayout(layoutStream)

	done := createTestSparseFileLayout(name, size, layout1)
	layoutTmp := unstreamLayout(layout2)
	<-done
	log.Info("Done writing layout of ", len(layoutTmp), "items")

	layout := streamLayout(layoutTmp)
	err := checkTestSparseFileLayout(name, layout)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRandomLayout100MB(t *testing.T) {
	const seed = 0
	const size = 100 /*MB*/ << 20
	prefix := "ssync"
	name := tempFileName(prefix)
	layoutStream := generateLayout(prefix, size, seed)
	layout1, layout2 := teeLayout(layoutStream)

	done := createTestSparseFileLayout(name, size, layout1)
	layoutTmp := unstreamLayout(layout2)
	<-done
	log.Info("Done writing layout of ", len(layoutTmp), "items")

	layout := streamLayout(layoutTmp)
	err := checkTestSparseFileLayout(name, layout)
	if err != nil {
		t.Fatal(err)
	}
}

func tempFileName(prefix string) string {
	// Make a temporary file name
	f, err := ioutil.TempFile("", prefix)
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

func unstreamLayout(in <-chan TestFileInterval) []TestFileInterval {
	layout := make([]TestFileInterval, 0, 4096)
	for i := range in {
		log.Trace("unstream", i)
		layout = append(layout, i)
	}
	return layout
}

func streamLayout(in []TestFileInterval) (out chan TestFileInterval) {
	out = make(chan TestFileInterval, 128)

	go func() {
		for _, i := range in {
			log.Trace("stream", i)
			out <- i
		}
		close(out)
	}()

	return out
}

func teeLayout(in <-chan TestFileInterval) (out1 chan TestFileInterval, out2 chan TestFileInterval) {
	out1 = make(chan TestFileInterval, 128)
	out2 = make(chan TestFileInterval, 128)

	go func() {
		for i := range in {
			log.Trace("Tee1...")
			out1 <- i
			log.Trace("Tee2...")
			out2 <- i
		}
		close(out1)
		close(out2)
	}()

	return out1, out2
}

func generateLayout(prefix string, size, seed int64) <-chan TestFileInterval {
	const maxInterval = 256 // Blocks
	layoutStream := make(chan TestFileInterval, 128)
	r := rand.New(rand.NewSource(seed))

	go func() {
		offset := int64(0)
		for offset < size {
			blocks := int64(r.Intn(maxInterval)) + 1 // 1..maxInterval
			length := blocks * Blocks
			if offset+length > size {
				// don't overshoot size
				length = size - offset
			}

			interval := Interval{offset, offset + length}
			offset += interval.Len()

			kind := SparseHole
			var mask byte
			if r.Intn(2) == 0 {
				// Data
				kind = SparseData
				mask = 0xAA * byte(r.Intn(10)/9) // 10%
			}
			t := TestFileInterval{FileInterval{kind, interval}, mask}
			log.Debug(prefix, t)
			layoutStream <- t
		}
		close(layoutStream)
	}()

	return layoutStream
}

func makeIntervalData(interval TestFileInterval) []byte {
	data := make([]byte, interval.Len())
	if SparseData == interval.Kind {
		for i := range data {
			data[i] = interval.dataMask ^ byte(interval.Begin/Blocks+1)
		}
	}
	return data
}

func createTestSparseFileLayout(name string, fileSize int64, layout <-chan TestFileInterval) (done chan struct{}) {
	done = make(chan struct{})

	// Fill up file with layout data
	go func() {
		f, err := os.Create(name)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		err = f.Truncate(fileSize)
		if err != nil {
			log.Fatal(err)
		}

		for interval := range layout {
			log.Debug("writing...", interval)
			if SparseData == interval.Kind {
				size := batch * Blocks
				for offset := interval.Begin; offset < interval.End; {
					if offset+size > interval.End {
						size = interval.End - offset
					}
					chunkInterval := TestFileInterval{FileInterval{SparseData, Interval{offset, offset + size}}, interval.dataMask}
					data := makeIntervalData(chunkInterval)
					_, err = f.WriteAt(data, offset)
					if err != nil {
						log.Fatal(err)
					}
					offset += size
				}
			}
		}
		f.Sync()
		close(done)
	}()

	return done
}

func checkTestSparseFileLayout(name string, layout <-chan TestFileInterval) error {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Read and check data
	for interval := range layout {
		log.Debug("checking...", interval)
		if SparseData == interval.Kind {
			size := batch * Blocks
			for offset := interval.Begin; offset < interval.End; {
				if offset+size > interval.End {
					size = interval.End - offset
				}
				dataModel := makeIntervalData(TestFileInterval{FileInterval{SparseData, Interval{offset, offset + size}}, interval.dataMask})
				data := make([]byte, size)
				f.ReadAt(data, offset)
				offset += size

				if !bytes.Equal(data, dataModel) {
					return errors.New(fmt.Sprint("data equality check failure at", interval))
				}
			}
		} else if SparseHole == interval.Kind {
			layoutActual, err := RetrieveLayout(f, interval.Interval)
			if err != nil {
				return errors.New(fmt.Sprint("hole retrieval failure at", interval, err))
			}
			if len(layoutActual) != 1 {
				return errors.New(fmt.Sprint("hole check failure at", interval))
			}
			if layoutActual[0] != interval.FileInterval {
				return errors.New(fmt.Sprint("hole equality check failure at", interval))
			}
		}
	}
	return nil // success
}
