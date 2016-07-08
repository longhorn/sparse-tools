package sparse

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
)

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

// we should disable this when we are confident
const enableFinalMD5 = true

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int) ([]byte, error) {
	var hashLocal []byte
	var err error
	for retries := 1; retries >= 0; retries-- {
		hashLocal, err = syncFile(localPath, addr, remotePath, timeout, retries > 0)
		if err != nil {
			log.Error("SSync error:", err)
		}
		break
	}
	return hashLocal, err
}

func syncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int, retry bool) ([]byte, error) {
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		log.Errorf("Failed to get size of local source file: %s, err: %s", localPath, err)
		return nil, err
	}
	fileSize := fileInfo.Size()
	directIO := (fileSize%Blocks == 0)
	log.Debugf("setting up directIo: %v", directIO)

	var fileIo FileIoProcessor
	if directIO {
		fileIo, err = NewDirectFileIoProcessor(localPath, os.O_RDONLY, 0)
	} else {
		fileIo, err = NewBufferedFileIoProcessor(localPath, os.O_RDONLY, 0)
	}
	if err != nil {
		log.Error("Failed to open local source file:", localPath)
		return nil, err
	}
	defer fileIo.Close()

	conn := connect(addr.Host, strconv.Itoa(int(addr.Port)), timeout)
	if nil == conn {
		err = fmt.Errorf("Failed to connect to %v", addr)
		log.Error(err)
		return nil, err
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

	session := &SyncSessionClient{encoder, decoder}
	err = session.sendSyncRequest(remotePath, fileSize)
	if err != nil {
		log.Errorf("Sync request to %s failed, error: %s", remotePath, err)
		return nil, err
	}

	err = session.syncFileContent(fileIo, fileSize)
	if err != nil {
		log.Error("syncFileContent failed: ", err)
	}

	if enableFinalMD5 && !AreFilesEqual(localPath, remotePath) {
		log.Error("After syncFileContent, local and remote files are different")
		return nil, fmt.Errorf("file: %s differs from file: %s", localPath, remotePath)
	}

	return nil, nil
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

type SyncSessionClient struct {
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func (session *SyncSessionClient) send(e interface{}) error {
	return session.encoder.Encode(e)
}

func (session *SyncSessionClient) receive(e interface{}) error {
	return session.decoder.Decode(e)
}

func (session *SyncSessionClient) syncFileContent(file FileIoProcessor, fileSize int64) error {
	exts, err := GetFiemapExtents(file)
	if err != nil {
		return err
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
			err := session.SyncHoleInterval(holeInterval)
			if err != nil {
				log.Debugf("SyncHoleInterval failed: %s", holeInterval)
				return err
			}
		}
		// report data
		log.Debugf("Here is a data: %s", interval)
		lastIntervalEnd = interval.End

		// syncing data interval
		err := session.SyncDataInterval(file, interval)
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
				err = session.SyncHoleInterval(holeInterval)
				if err != nil {
					log.Debugf("SyncHoleInterval failed: %s", holeInterval)
					return err
				}
			}
		}
	}

	// special case, the whole file is a hole
	if len(exts) == 0 && fileSize != 0 {
		// report hole
		holeInterval = Interval{0, fileSize}
		log.Debugf("The file is a hole: %s", holeInterval)

		// syncing hole interval
		err := session.SyncHoleInterval(holeInterval)
		if err != nil {
			log.Debugf("SyncHoleInterval failed: %s", holeInterval)
			return err
		}
	}

	return session.send(requestHeader{requestMagic, syncDone})
}

func (session *SyncSessionClient) sendSyncRequest(path string, size int64) error {
	err := session.send(requestHeader{requestMagic, syncRequestCode})
	if err != nil {
		log.Debug("send requestHeader failed: ", err)
		return err
	}
	err = session.send(path)
	if err != nil {
		log.Debug("send path failed: ", err)
		return err
	}
	err = session.send(size)
	if err != nil {
		log.Debug("send size failed: ", err)
		return err
	}

	var ack bool
	err = session.receive(&ack)
	if err != nil {
		log.Debug("decode ack from server failed: ", err)
		return err
	}
	log.Debugf("got SyncRequest ack back: %v", ack)
	if ack == false {
		return fmt.Errorf("Got negative ack from server")
	}

	return nil
}

func (session *SyncSessionClient) SyncHoleInterval(holeInterval Interval) error {
	/*
		sync hole interval:

		1. send the hole range(start and end byte offsets) to ensure the other end
			has pure hole in this range. If not, it will punch hole for the entire range
			then it will asking for continue
	*/
	log.Debug("syncing hole: ", holeInterval)
	err := session.send(requestHeader{requestMagic, syncHole})
	if err != nil {
		log.Debug("encode syncHole cmd failed: ", err)
		return err
	}
	err = session.send(holeInterval)
	if err != nil {
		log.Debugf("encode holeInterval: %s failed: %s", holeInterval, err)
		return err
	}
	var reply replyHeader
	err = session.receive(&reply)
	if err != nil {
		log.Debug("decode syncHole cmd reply failed: ", err)
		return err
	}
	if reply.Code == continueSync {
		log.Debug("got continueSync reply from server")
	}
	return nil
}

func (session *SyncSessionClient) SyncDataInterval(file FileIoProcessor, dataInterval Interval) error {
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
		err := session.send(requestHeader{requestMagic, syncData})
		if err != nil {
			log.Debug("encode syncData cmd reply failed: ", err)
			return err
		}
		err = session.send(batchInterval)
		if err != nil {
			log.Debug("encode batchInterval: %s failed, err: %s", batchInterval, err)
			return err
		}

		notInSync := true
		for notInSync {
			log.Debug("decoding ...")
			var reply replyHeader
			err := session.receive(&reply)
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
				err = session.send(localCheckSum)
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
				err = session.send(dataBuffer)
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
