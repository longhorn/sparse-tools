package sparse

import (
	"os"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

// FoldFile folds child snapshot data into its parent
func FoldFile(childFileName, parentFileName string) error {

	childFInfo, err := os.Stat(childFileName)
	if err != nil {
		panic("os.Stat(childFileName) failed, error: " + err.Error())
	}
	parentFInfo, err := os.Stat(parentFileName)
	if err != nil {
		panic("os.Stat(parentFileName) failed, error: " + err.Error())
	}

	// ensure no directory
	if childFInfo.IsDir() || parentFInfo.IsDir() {
		panic("at least one file is directory, not a normal file")
	}

	// ensure file sizes are equal
	if childFInfo.Size() != parentFInfo.Size() {
		panic("file sizes are not equal")
	}

	// open child and parent files
	childFileIo, err := NewDirectFileIoProcessor(childFileName, os.O_RDONLY, 0)
	if err != nil {
		panic("Failed to open childFile, error: " + err.Error())
	}
	defer childFileIo.Close()

	parentFileIo, err := NewDirectFileIoProcessor(parentFileName, os.O_WRONLY, 0)
	if err != nil {
		panic("Failed to open parentFile, error: " + err.Error())
	}
	defer parentFileIo.Close()

	return coalesce(parentFileIo, childFileIo)
}

func coalesce(parentFileIo FileIoProcessor, childFileIo FileIoProcessor) error {
	blockSize, err := getFileSystemBlockSize(childFileIo)
	if err != nil {
		panic("can't get FS block size, error: " + err.Error())
	}
	var data, hole int64
	for {
		data, _ = childFileIo.Seek(hole, seekData)
		if data < hole {
			break
		}
		hole, err = childFileIo.Seek(data, seekHole)
		if err != nil {
			log.Error("Failed to syscall.Seek SEEK_HOLE")
			return err
		}

		// now we have a data start offset and length(hole - data)
		// let's read from child and write to parent file block by block
		_, err = parentFileIo.Seek(data, os.SEEK_SET)
		if err != nil {
			log.Error("Failed to os.Seek os.SEEK_SET")
			return err
		}

		offset := data
		buffer := AllocateAligned(blockSize)
		for offset != hole {
			// read a block from child, maybe use bufio or Reader stream
			n, err := childFileIo.fileReadAt(buffer, offset)
			if n != len(buffer) || err != nil {
				log.Error("Failed to read from childFile")
				return err
			}
			// write a block to parent
			n, err = parentFileIo.fileWriteAt(buffer, offset)
			if n != len(buffer) || err != nil {
				log.Error("Failed to write to parentFile")
				return err
			}
			offset += int64(n)
		}
	}

	return nil
}

// get the file system block size
func getFileSystemBlockSize(fileIo FileIoProcessor) (int, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(fileIo.Name(), &stat)
	return int(stat.Blksize), err
}
