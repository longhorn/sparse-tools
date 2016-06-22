package sparse

import (
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	log "github.com/Sirupsen/logrus"
)

type FileIoProcessor interface {
	// File I/O methods for direct or bufferend I/O
	fileReadAt(data []byte, offset int64) (int, error)
	fileWriteAt(data []byte, offset int64) (int, error)
	getFile() *os.File
	Close() error
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (ret int64, err error)
	Name() string
}

type BufferedFileIoProcessor struct {
	*os.File
}

func NewBufferedFileIoProcessor(name string, flag int, perm os.FileMode, isCreate ...bool) (*BufferedFileIoProcessor, error) {
	file, err := os.OpenFile(name, flag, perm)

	// if file does not exist, we need to create it if asked to
	if err != nil && len(isCreate) > 0 && isCreate[0] {
		file, err = os.Create(name)
	}
	if err != nil {
		return nil, err
	}

	return &BufferedFileIoProcessor{file}, nil
}

func NewBufferedFileIoProcessorByFP(fp *os.File) *BufferedFileIoProcessor {
	return &BufferedFileIoProcessor{fp}
}

func (file *BufferedFileIoProcessor) fileReadAt(data []byte, offset int64) (int, error) {
	return file.File.ReadAt(data, offset)
}

func (file *BufferedFileIoProcessor) fileWriteAt(data []byte, offset int64) (int, error) {
	return file.File.WriteAt(data, offset)
}

func (file *BufferedFileIoProcessor) getFile() *os.File {
	return file.File
}

type DirectFileIoProcessor struct {
	*os.File
}

const (
	// what to align the block buffer to
	alignment = 4096

	// BlockSize sic
	BlockSize = alignment
)

func NewDirectFileIoProcessor(name string, flag int, perm os.FileMode, isCreate ...bool) (*DirectFileIoProcessor, error) {
	file, err := os.OpenFile(name, syscall.O_DIRECT|flag, perm)

	// if file does not exist, we need to create it if asked to
	if err != nil && len(isCreate) > 0 && isCreate[0] {
		file, err = os.OpenFile(name, os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT|flag, perm)
	}
	if err != nil {
		return nil, err
	}

	return &DirectFileIoProcessor{file}, nil
}

func NewDirectFileIoProcessorByFP(fp *os.File) *DirectFileIoProcessor {
	return &DirectFileIoProcessor{fp}
}

// ReadAt read into unaligned data buffer via direct I/O
// Use AllocateAligned to avoid extra data fuffer copy
func (file *DirectFileIoProcessor) fileReadAt(data []byte, offset int64) (int, error) {
	if alignmentShift(data) == 0 {
		return file.File.ReadAt(data, offset)
	}
	buf := AllocateAligned(len(data))
	n, err := file.File.ReadAt(buf, offset)
	copy(data, buf)
	return n, err
}

// WriteAt write from unaligned data buffer via direct I/O
// Use AllocateAligned to avoid extra data fuffer copy
func (file *DirectFileIoProcessor) fileWriteAt(data []byte, offset int64) (int, error) {
	if alignmentShift(data) == 0 {
		return file.File.WriteAt(data, offset)
	}
	// Write unaligned
	buf := AllocateAligned(len(data))
	copy(buf, data)
	n, err := file.File.WriteAt(buf, offset)
	return n, err
}

func (file *DirectFileIoProcessor) getFile() *os.File {
	return file.File
}

// AllocateAligned returns []byte of size aligned to alignment
func AllocateAligned(size int) []byte {
	block := make([]byte, size+alignment)
	shift := alignmentShift(block)
	offset := 0
	if shift != 0 {
		offset = alignment - shift
	}
	block = block[offset:size]
	shift = alignmentShift(block)
	if shift != 0 {
		panic("Alignment failure")
	}
	return block
}

// alignmentShift returns alignment of the block in memory
func alignmentShift(block []byte) int {
	if len(block) == 0 {
		return 0
	}
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(alignment-1))
}

func ReadDataInterval(file FileIoProcessor, dataInterval Interval) ([]byte, error) {
	log.Debug("reading data from file ...")
	data := make([]byte, dataInterval.Len())
	n, err := file.fileReadAt(data, dataInterval.Begin)
	if err != nil {
		if err == io.EOF {
			log.Debug("have read at the end of file, total read: ", n)
		} else {
			errStr := fmt.Sprintf("File read error: %s", err)
			log.Error(errStr)
			return nil, fmt.Errorf(errStr)
		}
	}
	log.Debug("reading data from file is done")
	return data[:n], nil
}

func WriteDataInterval(file FileIoProcessor, dataInterval Interval, data []byte) error {
	log.Debug("writing data ...")
	_, err := file.fileWriteAt(data, dataInterval.Begin)
	if err != nil {
		errStr := fmt.Sprintf("Failed to write file using interval:%s, error: %s", dataInterval, err)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	log.Debug("written data")
	return nil
}

func HashDataInterval(file FileIoProcessor, dataInterval Interval) ([]byte, error) {
	data, err := ReadDataInterval(file, dataInterval)
	if err != nil {
		return nil, err
	}
	sum := sha512.Sum512(data)
	return sum[:], nil
}

func GetFiemapExtents(file FileIoProcessor) ([]Extent, error) {
	var exts []Extent
	fiemap := NewFiemapFile(file.getFile())

	// first call of Fiemap with 0 extent count will actually return total mapped ext counts
	// we can use that to allocate extent struct slice to get details of each extent
	extCount, _, errno := fiemap.Fiemap(0)
	if errno != 0 {
		log.Error("failed to call fiemap.Fiemap(0)")
		return exts, fmt.Errorf(errno.Error())
	}
	log.Debugf("extCount: %d", extCount)

	if extCount != 0 {
		var errno syscall.Errno
		_, exts, errno = fiemap.Fiemap(extCount)
		if errno != 0 {
			log.Error("failed to call fiemap.Fiemap(extCount)")
			return exts, fmt.Errorf(errno.Error())
		}
	}

	return exts, nil
}
