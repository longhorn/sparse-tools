package sparse

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
	fio "github.com/rancher/sparse-tools/directfio"
)

// File I/O methods for direct or bufferend I/O
var fileOpen func(name string, flag int, perm os.FileMode) (*os.File, error)
var fileReadAt func(file *os.File, data []byte, offset int64) (int, error)
var fileWriteAt func(file *os.File, data []byte, offset int64) (int, error)

func fileBufferedOpen(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}
func fileBufferedReadAt(file *os.File, data []byte, offset int64) (int, error) {
	return file.ReadAt(data, offset)
}
func fileBufferedWriteAt(file *os.File, data []byte, offset int64) (int, error) {
	return file.WriteAt(data, offset)
}

// SetupFileIO Sets up direct file I/O or buffered for small unaligned files
func SetupFileIO(direct bool) {
	if direct {
		fileOpen = fio.OpenFile
		fileReadAt = fio.ReadAt
		fileWriteAt = fio.WriteAt
		log.Debug("Mode: directfio")
	} else {
		fileOpen = fileBufferedOpen
		fileReadAt = fileBufferedReadAt
		fileWriteAt = fileBufferedWriteAt
		log.Debug("Mode: buffered")
	}
}

func ReadDataInterval(file *os.File, dataInterval Interval) ([]byte, error) {
	log.Debug("reading data from file ...")
	data := make([]byte, dataInterval.Len())
	n, err := fileReadAt(file, data, dataInterval.Begin)
	if err != nil {
		if err == io.EOF {
			log.Debug("have read at the end of file, total read: ", n)
		} else {
			errStr := fmt.Sprintf("File read error: %s", err)
			log.Error(errStr)
			return nil, fmt.Errorf(errStr)
		}
	} else if int64(n) != dataInterval.Len() {
		errStr := "File read underrun"
		log.Error(errStr)
		return nil, fmt.Errorf(errStr)
	}
	log.Debug("reading data from file is done")
	return data[:n], nil
}

func WriteDataInterval(file *os.File, dataInterval Interval, data []byte) error {
	log.Debug("writing data ...")
	_, err := fileWriteAt(file, data, dataInterval.Begin)
	if err != nil {
		errStr := fmt.Sprintf("Failed to write file using interval:%s, error: %s", dataInterval, err)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	log.Debug("written data")
	return nil
}

func HashDataInterval(file *os.File, dataInterval Interval) ([]byte, error) {
	data, err := ReadDataInterval(file, dataInterval)
	if err != nil {
		return nil, err
	}
	hasher := sha1.New()
	hasher.Write(data)
	hash := hasher.Sum(nil)

	return hash, nil
}
