package sparse

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
)

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

const connectionRetries = 5
const verboseClient = true

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int) (hashLocal []byte, err error) {
	for retries := 1; retries >= 0; retries-- {
		hashLocal, err = syncFile(localPath, addr, remotePath, timeout, retries > 0)
		if err != nil {
			log.Error("SSync error:", err)
		}
		break
	}
	return
}

func syncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int, retry bool) ([]byte, error) {
	file, err := os.OpenFile(localPath, os.O_RDONLY, 0)
	if err != nil {
		log.Error("Failed to open local source file:", localPath)
		return nil, err
	}
	log.Debugf("opened file: %s", localPath)

	defer file.Close()

	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Error("Failed to get size of local source file:", localPath, err)
		return nil, err
	}

	directIO := (fileSize%Blocks == 0)
	SetupFileIO(directIO)
	log.Debugf("setting up directIo: %v", directIO)
	if directIO {
		file.Close()
		file, err = fileOpen(localPath, os.O_RDONLY, 0)
		if err != nil {
			log.Error("Failed to open local source file for direct IO:", localPath)
			return nil, err
		}
	}

	conn := connect(addr.Host, strconv.Itoa(int(addr.Port)), timeout)
	if nil == conn {
		err = fmt.Errorf("Failed to connect to %v", addr)
		log.Error(err)
		return nil, err
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	ack, err := sendSyncRequest(encoder, decoder, remotePath, fileSize)
	if err != nil || ack == false {
		log.Errorf("Sync request to %s failed, error: %s, ack: %v", remotePath, err, ack)
		return nil, err
	}

	err = syncFileContent(encoder, decoder, file, fileSize)
	if err != nil {
		log.Error("syncFileContent failed: ", err)
	}
	return nil, err
}

func connect(host, port string, timeout int) net.Conn {
	// connect to this socket
	endpoint := host + ":" + port
	raddr, err := net.ResolveTCPAddr("tcp", endpoint)
	if err != nil {
		log.Fatal("Connection address resolution error:", err)
	}
	timeStart := time.Now()
	timeStop := timeStart.Add(time.Duration(timeout) * time.Second)
	for timeNow := timeStart; timeNow.Before(timeStop); timeNow = time.Now() {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			log.Info("connected to server")
			return conn
		}
		log.Warn("Failed connection to", endpoint, "Retrying...")
		if timeNow != timeStart {
			// only sleep after the second attempt to speedup tests
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

func syncFileContent(encoder *gob.Encoder, decoder *gob.Decoder, file *os.File, fileSize int64) error {

	fiemap := NewFiemapFile(file)

	// first call of Fiemap with 0 extent count will actually return total mapped ext counts
	// we can use that to allocate extent struct slice to get details of each extent
	extCount, _, errno := fiemap.Fiemap(0)
	if errno != 0 {
		log.Error("failed to call fiemap.Fiemap(0)")
		return fmt.Errorf(errno.Error())
	}
	log.Debugf("extCount: %d", extCount)

	var exts []Extent
	if extCount != 0 {
		var errno syscall.Errno
		_, exts, errno = fiemap.Fiemap(extCount)
		if errno != 0 {
			log.Error("failed to call fiemap.Fiemap(extCount)")
			return fmt.Errorf(errno.Error())
		}
		log.Debugf("got extents[]: %d", len(exts))
	}

	var lastIntervalEnd int64
	var holeInterval Interval

	// Process each extent
	for index, e := range exts {
		interval := Interval{int64(e.Logical), int64(e.Logical + e.Length)}
		log.Debugf("Extent: %s, %x", interval, e.Flags)

		if lastIntervalEnd < interval.Begin {
			// report hole
			holeInterval = Interval{lastIntervalEnd, interval.Begin}
			log.Debugf("Here is a hole: %s", holeInterval)

			// syncing hole interval
			err := SyncHoleInterval(encoder, decoder, holeInterval)
			if err != nil {
				log.Debugf("SyncHoleInterval failed: %s", holeInterval)
				return err
			}
		}
		// report data
		log.Debugf("Here is a data: %s", interval)
		lastIntervalEnd = interval.End

		// syncing data interval
		err := SyncDataInterval(encoder, decoder, file, interval)
		if err != nil {
			log.Debugf("SyncDataInterval failed: %s", interval)
			return err
		}
		if e.Flags&FIEMAP_EXTENT_LAST != 0 {
			log.Debugf("hit FIEMAP_EXTENT_LAST flag, are we on last index yet ? %v", (index == len(exts)-1))

			// report last hole
			if lastIntervalEnd < fileSize {
				holeInterval := Interval{lastIntervalEnd, fileSize}
				log.Debugf("Here is a hole: %s", holeInterval)

				// syncing hole interval
				err = SyncHoleInterval(encoder, decoder, holeInterval)
				if err != nil {
					log.Debugf("SyncHoleInterval failed: %s", holeInterval)
					return err
				}
			}
		}
	}

	// special case, the whole file is a hole
	if extCount == 0 {
		// report hole
		holeInterval = Interval{0, fileSize}
		log.Debugf("The file is a hole: %s", holeInterval)

		// syncing hole interval
		err := SyncHoleInterval(encoder, decoder, holeInterval)
		if err != nil {
			log.Debugf("SyncHoleInterval failed: %s", holeInterval)
			return err
		}
	}

	encoder.Encode(requestHeader{requestMagic, syncDone})
	return nil
}

func sendSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) (bool, error) {
	err := encoder.Encode(requestHeader{requestMagic, syncRequestCode})
	if err != nil {
		log.Debug("send requestHeader failed: ", err)
		return false, err
	}
	err = encoder.Encode(path)
	if err != nil {
		log.Debug("send path failed: ", err)
		return false, err
	}
	err = encoder.Encode(size)
	if err != nil {
		log.Debug("send size failed: ", err)
		return false, err
	}

	var ack bool
	err = decoder.Decode(&ack)
	if err != nil {
		log.Debug("decode ack from server failed: ", err)
		return false, err
	}
	log.Debugf("got SyncRequest ack back: %v", ack)

	return ack, nil
}

func SyncHoleInterval(encoder *gob.Encoder, decoder *gob.Decoder, holeInterval Interval) error {
	/*
		sync hole interval:

		1. send the hole range(start and end byte offsets) to ensure the other end
			has pure hole in this range. If not, it will punch hole for the entire range
			then it will asking for continue
	*/
	log.Debug("syncing hole: ", holeInterval)
	err := encoder.Encode(requestHeader{requestMagic, syncHole})
	if err != nil {
		log.Debug("encode syncHole cmd failed: ", err)
		return err
	}
	err = encoder.Encode(holeInterval)
	if err != nil {
		log.Debugf("encode holeInterval: %s failed: %s", holeInterval, err)
		return err
	}
	var reply replyHeader
	err = decoder.Decode(&reply)
	if err != nil {
		log.Debug("decode syncHole cmd reply failed: ", err)
		return err
	}
	if reply.Code != continueSync {
		log.Debug("got unexpected reply from server: ", reply.Code)
	} else {
		log.Debug("got continueSync reply from server")
	}
	return nil
}

func SyncDataInterval(encoder *gob.Encoder, decoder *gob.Decoder, file *os.File, dataInterval Interval) error {
	const batch = 32 * Blocks

	// Process data in chunks
	for offset := dataInterval.Begin; offset < dataInterval.End; {
		size := batch
		if offset+size > dataInterval.End {
			size = dataInterval.End - offset
		}
		batchInterval := Interval{offset, offset + size}

		/*
			sync the batch data interval:

			1. send the data range(start and end byte offsets) to ensure the other end
				has pure data in this range, if so, the other end will ask for checksum
				and calculate its own at the same time. If not, it will ask for data
			2. waiting for the reply asking checksum or data
			3. if the other end asking for checksum, then calculate checksum and send checksum
			4. if the other end asking for data, then send data
			5. if the other end asking continue, then data interval is in sync and move on
				to the next batch interval
		*/
		log.Debug("syncing data batch interval: ", batchInterval)
		err := encoder.Encode(requestHeader{requestMagic, syncData})
		if err != nil {
			log.Debug("encode syncData cmd reply failed: ", err)
			return err
		}
		err = encoder.Encode(batchInterval)
		if err != nil {
			log.Debug("encode batchInterval: %s failed, err: %s", batchInterval, err)
			return err
		}

		notInSync := true
		for notInSync {
			log.Debug("decoding ...")
			var reply replyHeader
			err := decoder.Decode(&reply)
			if err != nil {
				log.Debug("decoder syncData cmd reply failed: ", err)
				return err
			}
			switch reply.Code {
			case sendChecksum:
				log.Debug("server reply asking for checksum")

				// calculate checksum for the data batch interval
				localCheckSum, err := HashDataInterval(file, batchInterval)
				if err != nil {
					log.Errorf("HashDataInterval locally: %s failed, err: %s", batchInterval, err)
					return err
				}

				// send the checksum
				log.Debug("sending checksum")
				err = encoder.Encode(localCheckSum)
				if err != nil {
					log.Errorf("encode localCheckSum for data interval: %s failed, err: %s", batchInterval, err)
					return err
				}
			case sendData:
				log.Debug("server reply asking for data")

				// read data from the file
				dataBuffer, err := ReadDataInterval(file, batchInterval)
				if err != nil {
					log.Errorf("ReadDataInterval locally: %s failed, err: %s", batchInterval, err)
					return err
				}

				// send data buffer
				log.Debugf("sending dataBuffer size: %d", len(dataBuffer))
				err = encoder.Encode(dataBuffer)
				if err != nil {
					log.Errorf("encode dataBuffer for data interval: %s failed, err: %s", batchInterval, err)
					return err
				}
			case continueSync:
				log.Debug("server is reporting in-sync with data batch interval")
				notInSync = false
			}
		}
		log.Debug("syncing data batch interval is done")
		offset += batchInterval.Len()
	}
	return nil
}
