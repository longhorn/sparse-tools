package sparse

import "os"
import "syscall"

// Interval [Begin, End) is non-inclusive at the End
type Interval struct {
	Begin, End int64
}

// Len returns length of Interval
func (interval Interval) Len() int64 {
	return interval.End - interval.Begin
}

// FileIntervalKind distinguishes between data and hole
type FileIntervalKind int

// Sparse file Interval types
const (
	SparseData FileIntervalKind = 1 + iota
	SparseHole
    SparseIgnore // ignore file interval (equal src vs dst part)
)

// FileInterval describes either sparse data Interval or a hole
type FileInterval struct {
	Kind FileIntervalKind
	Interval
}

// Storage block size in bytes
const (
	Blocks int64 = 4 << 10 // 4k
)

// os.Seek sparse whence values.
const (
	// Adjust the file offset to the next location in the file
	// greater than or equal to offset containing data.  If offset
	// points to data, then the file offset is set to offset.
	seekData int = 3

	// Adjust the file offset to the next hole in the file greater
	// than or equal to offset.  If offset points into the middle of
	// a hole, then the file offset is set to offset.  If there is no
	// hole past offset, then the file offset is adjusted to the End
	// of the file (i.e., there is an implicit hole at the End of any
	// file).
	seekHole int = 4
)

// syscall.Fallocate mode bits
const (
	// default is extend size
	fallocFlKeepSize uint32 = 1

	// de-allocates range
	fallocFlPunchHole uint32 = 2
)

// RetrieveLayout retrieves sparse file hole and data layout
func RetrieveLayout(file *os.File, r Interval) ([]FileInterval, error) {
	layout := make([]FileInterval, 0, 128)
	curr := r.Begin

	// Data or hole?
	offsetData, errData := file.Seek(curr, seekData)
	offsetHole, errHole := file.Seek(curr, seekHole)
	var interval FileInterval
	if errData != nil {
		// Hole only
		interval = FileInterval{SparseHole, Interval{curr, r.End}}
		if interval.Len() > 0 {
			layout = append(layout, interval)
		}
		return layout, nil
	} else if errHole != nil {
		// Data only
		interval = FileInterval{SparseData, Interval{curr, r.End}}
		if interval.Len() > 0 {
			layout = append(layout, interval)
		}
		return layout, nil
	}

	if offsetData < offsetHole {
		interval = FileInterval{SparseData, Interval{curr, offsetHole}}
		curr = offsetHole
	} else {
		interval = FileInterval{SparseHole, Interval{curr, offsetData}}
		curr = offsetData
	}
	if interval.Len() > 0 {
		layout = append(layout, interval)
	}

	for curr < r.End {
		var whence int
		if SparseData == interval.Kind {
			whence = seekData
		} else {
			whence = seekHole
		}

		// Note: file.Seek masks syscall.ENXIO hence syscall is used instead
		next, errno := syscall.Seek(int(file.Fd()), curr, whence)
		if errno != nil {
			switch errno {
			case syscall.ENXIO:
				// no more intervals
				next = r.End // close the last interval
			default:
				// mimic standard "os"" package error handler
				return nil, &os.PathError{Op: "seek", Path: file.Name(), Err: errno}
			}
		}
		if SparseData == interval.Kind {
			// End of data, handle the last hole if any
			interval = FileInterval{SparseHole, Interval{curr, next}}
		} else {
			// End of hole, handle the last data if any
			interval = FileInterval{SparseData, Interval{curr, next}}
		}
		curr = next
		if interval.Len() > 0 {
			layout = append(layout, interval)
		}
	}
	return layout, nil
}

// PunchHole in a sparse file, preserve file size
func PunchHole(file *os.File, hole Interval) error {
	fd := int(file.Fd())
	mode := fallocFlPunchHole | fallocFlKeepSize
	return syscall.Fallocate(fd, mode, hole.Begin, hole.Len())
}
