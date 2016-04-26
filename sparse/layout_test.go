package sparse

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
)

const name = "foo.bar"

func TestLayout0(t *testing.T) {
	layoutModel := []FileInterval{}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout1(t *testing.T) {
	layoutModel := []FileInterval{{SparseHole, Interval{0, 4 * Blocks}}}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout2(t *testing.T) {
	layoutModel := []FileInterval{{SparseData, Interval{0, 4 * Blocks}}}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout3(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 4 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout4(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 4 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout5(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout6(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout7(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseData, Interval{0, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutExpected)
}

func TestLayout8(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutExpected)
}

func TestPunchHole0(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseHole, Interval{0 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	punchHoleTest(t, name, layoutModel, Interval{0, 1 * Blocks}, layoutExpected)
}

func layoutTest(t *testing.T, name string, layoutModel, layoutExpected []FileInterval) {
	createTestSparseFile(name, layoutModel)

	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Fatal(err)
	}

	layoutActual, err := RetrieveLayout(f, Interval{0, size})
	if err != nil || !reflect.DeepEqual(layoutExpected, layoutActual) {
		t.Fatal("wrong sparse layout")
	}

	if checkTestSparseFile(name, layoutModel) != nil {
		t.Fatal("wrong sparse layout content")
	}
	os.Remove(name)
}

func punchHoleTest(t *testing.T, name string, layoutModel []FileInterval, hole Interval, layoutExpected []FileInterval) {
	createTestSparseFile(name, layoutModel)

	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Fatal(err)
	}

	err = PunchHole(f, hole)
	if err != nil {
		t.Fatal(err)
	}

	layoutActual, err := RetrieveLayout(f, Interval{0, size})
	if err != nil || !reflect.DeepEqual(layoutExpected, layoutActual) {
		t.Fatal("wrong sparse layout")
	}

	os.Remove(name)
}

func makeData(interval FileInterval) []byte {
	data := make([]byte, interval.Len())
	if SparseData == interval.Kind {
		for i := range data {
			data[i] = byte(interval.Begin/Blocks + 1)
		}
	}
	return data
}

func createTestSparseFile(name string, layout []FileInterval) {
	f, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if 0 == len(layout) {
		return // empty file
	}

	// Fill up data
	for _, interval := range layout {
		if SparseData == interval.Kind {
			data := makeData(interval)
			f.WriteAt(data, interval.Begin)
		}
	}

	// Resize the file to the last hole
	last := len(layout) - 1
	if SparseHole == layout[last].Kind {
		if err := f.Truncate(layout[last].End); err != nil {
			log.Fatal(err)
		}
	}

	f.Sync()
}

func checkTestSparseFile(name string, layout []FileInterval) error {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if 0 == len(layout) {
		return nil // empty file
	}

	// Read and check data
	for _, interval := range layout {
		if SparseData == interval.Kind {
			dataModel := makeData(interval)
			data := make([]byte, interval.Len())
			f.ReadAt(data, interval.Begin)
			if !bytes.Equal(data, dataModel) {
				return errors.New(fmt.Sprint("data equality check failure at", interval))
			}
		} else if SparseHole == interval.Kind {
			layoutActual, err := RetrieveLayout(f, interval.Interval)
			if err != nil {
				return errors.New(fmt.Sprint("hole retrieval failure at", interval, err))
			}
			if len(layoutActual) != 1 {
				return errors.New(fmt.Sprint("hole check failure at", interval))
			}
			if layoutActual[0] != interval {
				return errors.New(fmt.Sprint("hole equality check failure at", interval))
			}
		}
	}
	return nil // success
}
