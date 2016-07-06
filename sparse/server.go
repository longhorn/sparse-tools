package sparse

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
)

type requestCode int

const (
	requestMagic    requestCode = 31415926
	syncRequestCode requestCode = iota
	syncHole
	syncData
	syncDone
)

func (reqCode requestCode) String() string {
	var s string
	if reqCode == syncRequestCode {
		s = "syncRequestCode"
	} else if reqCode == syncHole {
		s = "syncHole"
	} else if reqCode == syncData {
		s = "syncData"
	} else if reqCode == syncDone {
		s = "syncDone"
	}
	return s
}

type requestHeader struct {
	Magic requestCode
	Code  requestCode
}

func (req requestHeader) String() string {
	return fmt.Sprintf("Request [Magic: %s, Code: %s]", req.Magic, req.Code)
}

type replyCode int

const (
	replyMagic   replyCode = 31415928
	continueSync replyCode = iota
	sendChecksum
	sendData
)

func (repCode replyCode) String() string {
	var s string
	if repCode == continueSync {
		s = "continueSync"
	} else if repCode == sendChecksum {
		s = "sendChecksum"
	} else if repCode == sendData {
		s = "sendData"
	}
	return s
}

type replyHeader struct {
	Magic replyCode
	Code  replyCode
}

func (rep replyHeader) String() string {
	return fmt.Sprintf("Reply [Magic: %s, Code: %s]", rep.Magic, rep.Code)
}

// Server daemon
func Server(addr TCPEndPoint, timeout int) {
	server(addr, true /*serve single connection for now*/, timeout)
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(addr TCPEndPoint, timeout int) {
	server(addr, true, timeout)
}

func server(addr TCPEndPoint, serveOnce /*test flag*/ bool, timeout int) {
	serverConnectionTimeout := time.Duration(timeout) * time.Second
	// listen on all interfaces
	EndPoint := addr.Host + ":" + strconv.Itoa(int(addr.Port))
	laddr, err := net.ResolveTCPAddr("tcp", EndPoint)
	if err != nil {
		log.Fatal("Connection listener address resolution error:", err)
	}
	ln, err := net.ListenTCP("tcp", laddr)
	listenRetries := 5
	for ; err != nil && listenRetries > 0; listenRetries-- {
		log.Error("Connection listener error:", err)
		log.Error("retrying ...")
		ln, err = net.ListenTCP("tcp", laddr)
	}
	if err != nil && listenRetries == 0 {
		log.Fatal("Connection listener error:", err)
	}

	defer ln.Close()
	ln.SetDeadline(time.Now().Add(serverConnectionTimeout))
	log.Info("Sync server is up...")

	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			log.Fatal("Connection accept error:", err)
		}

		if serveOnce {
			// This is to avoid server listening port conflicts while running tests
			// exit after single connection request
			if serveConnection(conn) {
				break // no retries
			}
			log.Warn("Server: waiting for client sync retry...")
		} else {
			go serveConnection(conn)
		}
	}
	log.Info("Sync server exit.")
}

// returns true if no retry is necessary
func serveConnection(conn net.Conn) bool {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)
	session := &SyncSessionServer{encoder, decoder}

	return session.serveSession()
}

type SyncSessionServer struct {
	encoder *gob.Encoder
	decoder *gob.Decoder
}

func (session *SyncSessionServer) send(e interface{}) error {
	return session.encoder.Encode(e)
}

func (session *SyncSessionServer) receive(e interface{}) error {
	return session.decoder.Decode(e)
}

func (session *SyncSessionServer) serveSession() bool {
	var request requestHeader
	err := session.receive(&request)
	if err != nil {
		log.Error("Decode request error:", err)
		return true
	}
	if requestMagic != request.Magic {
		log.Error("Bad request, wrong Magic code in request")
		return true
	}

	switch request.Code {
	case syncRequestCode:
		var path string
		err := session.receive(&path)
		if err != nil {
			log.Error("decode path error:", err)
			return true
		}
		log.Debug("got the file path: ", path)

		var size int64
		err = session.receive(&size)
		if err != nil {
			log.Error("decode size error:", err)
			return true
		}
		log.Debugf("got the file size: %d", size)

		return session.serveSyncRequest(path, size)
	}
	return true
}

// returns true if no retry is necessary
func (session *SyncSessionServer) serveSyncRequest(path string, size int64) bool {
	directFileIO := size%Blocks == 0
	log.Debugf("setting up directIo: %v, size=%d", (size%Blocks == 0), size)

	var fileIo FileIoProcessor
	var err error
	if directFileIO {
		fileIo, err = NewDirectFileIoProcessor(path, os.O_RDWR, 0666, true)
	} else {
		fileIo, err = NewBufferedFileIoProcessor(path, os.O_RDWR, 0666, true)
	}
	if err != nil {
		log.Errorf("Failed to open/create local source file, path: %s, err: %s", path, err)
		err = session.send(false) // NACK request
		if err != nil {
			log.Error("encode ack error: ", err)
			return true
		}
		return true
	}

	// Setup close sequence, LIFO order
	defer fileIo.Close()
	if !directFileIO {
		defer fileIo.Sync()
	}

	// Resize the file
	if err = fileIo.Truncate(size); err != nil {
		log.Error("Failed to resize file:", string(path), err)
		err = session.send(false) // NACK request
		if err != nil {
			log.Error("encode ack error:", err)
			return true
		}

		return true
	}
	log.Debugf("truncated file into size: %d", size)

	// the starting point of interval sync request within originalFileIntervalLayout slice
	//index := 0
	localHoleIntervals, localDataIntervals, err := getFileLayout(fileIo)
	if err != nil {
		log.Error("Failed to getFileLayout", err)
		err = session.send(false) // NACK request
		if err != nil {
			log.Error("encode ack error:", err)
			return true
		}
		return true
	}
	log.Debugf("localHoleIntervals: %s", localHoleIntervals)
	log.Debugf("localDataIntervals: %s", localDataIntervals)

	// ack == true, so we can start syncing with file content from remote endpoint
	err = session.send(true)
	if err != nil {
		log.Error("encode ack error:", err)
		return true
	}

	// loop for getting request until all synced
	var request requestHeader
	moreRequest := true
	for moreRequest {
		log.Debug("decoding...")
		err := session.receive(&request)
		if err != nil {
			log.Error("decode requestHeader error:", err)
			return true
		}
		log.Debugf("request: %s", request)

		switch request.Code {

		case syncDone:
			moreRequest = false
			log.Debug("got syncDone")

		case syncHole:
			err := session.serveSyncHole(fileIo, localHoleIntervals)
			if err != nil {
				return true
			}

		case syncData:
			err := session.serveSyncData(fileIo, localDataIntervals)
			if err != nil {
				return true
			}
		}
	}

	return true
}

func (session *SyncSessionServer) serveSyncHole(file FileIoProcessor, localHoleIntervals []Interval) error {
	/*
		sync hole interval:

		1. get the hole range(start and end byte offsets)
		2. ensure the range is within pure hole extents. If not, it will punch hole for the entire range
		3. it will ask for continue
	*/
	var remoteHoleInterval Interval
	err := session.receive(&remoteHoleInterval)
	if err != nil {
		log.Error("decode remoteHoleInterval error:", err)
		return err
	}
	log.Debug("receiving remote hole interval: ", remoteHoleInterval)

	pureHole := false

	// do a binary search of the starting offset of dataInterval through the layout
	i := sort.Search(len(localHoleIntervals),
		func(i int) bool { return remoteHoleInterval.Begin < localHoleIntervals[i].Begin })
	log.Debug("found remoteHoleInterval to insert position in localHoleIntervals is: ", i)

	// i == 0 when insertion point is at the head, or len(originalFileIntervalLayout) == 0
	// so not within any data range for sure. Otherwise i > 0, the searching
	// point(dataInterval.Begin) is definitely less than originalFileIntervalLayout[i].Begin,
	// and also dataInterval.Begin >= originalFileIntervalLayout[i-1].Begin by f() closure.
	// So we just need to check if both Begin and End of dataInterval is <= originalFileIntervalLayout[i-1].End.
	// If so, then within that original data extent, otherwise not. Assumption here is:
	// adjacent data extents don't exist, they are all seperated by holes. If assumption
	// fails, we basically asking for data transfer directly without checking if checksum matches
	// or not. But that is just extra overhead. We know this assumption
	// doesn't fail often for sure. So this is acceptable.
	if i > 0 &&
		remoteHoleInterval.Begin <= localHoleIntervals[i-1].End &&
		remoteHoleInterval.End <= localHoleIntervals[i-1].End {
		log.Debugf("remoteHoleInterval %s is within localHoleIntervals", remoteHoleInterval)
		pureHole = true
	} else {
		log.Debugf("remoteHoleInterval %s is not within localHoleIntervals", remoteHoleInterval)
	}
	if !pureHole {
		log.Debug("punching hole:", remoteHoleInterval)
		fiemap := NewFiemapFile(file.getFile())
		err := fiemap.PunchHole(remoteHoleInterval.Begin, remoteHoleInterval.Len())
		if err != nil {
			log.Errorf("PunchHole: %s error: %s", remoteHoleInterval, err)
			return err
		}
	}

	err = session.send(replyHeader{replyMagic, continueSync})
	if err != nil {
		log.Error("encode replyHeader error:", err)
		return err
	}

	return nil
}

func (session *SyncSessionServer) serveSyncData(file FileIoProcessor, localDataIntervals []Interval) error {
	/*
		sync the batch data interval:

		1. send the data range(start and end byte offsets) to ensure the other end
			has pure data in this range, if so, the other end will ask for checksum
			and calculate its own at the same time. If not, it will ask for data
		2. waiting for the response saying checksum or data
		3. if the other end asking for checksum, then calculate checksum and send checksum
		4. if the other end asking for data, then send data
		5. if the other end asking continue, then data interval is in sync and move on
			to the next batch interval
	*/
	var dataInterval Interval
	err := session.receive(&dataInterval)
	if err != nil {
		log.Error("decode dataInterval error:", err)
		return err
	}
	log.Debug("receiving data interval: ", dataInterval)

	pureData := false

	// do a binary search of the starting offset of dataInterval through the layout
	i := sort.Search(len(localDataIntervals),
		func(i int) bool { return dataInterval.Begin < localDataIntervals[i].Begin })
	log.Debug("found position to insert position in localDataIntervals is: ", i)

	// i == 0 when insertion point is at the head, or len(originalFileIntervalLayout) == 0
	// so not within any data range for sure. Otherwise i > 0, the searching
	// point(dataInterval.Begin) is definitely less than originalFileIntervalLayout[i].Begin,
	// and also dataInterval.Begin >= originalFileIntervalLayout[i-1].Begin by f() closure.
	// So we just need to check if both Begin and End of dataInterval is <= originalFileIntervalLayout[i-1].End.
	// If so, then within that original data extent, otherwise not. Assumption here is:
	// adjacent data extents don't exist, they are all seperated by holes. If assumption
	// fails, we basically asking for data transfer directly without checking if checksum matches
	// or not. But that is just extra overhead. We know this assumption
	// doesn't fail often for sure. So this is acceptable.
	if i > 0 &&
		dataInterval.Begin <= localDataIntervals[i-1].End &&
		dataInterval.End <= localDataIntervals[i-1].End {
		log.Debugf("dataInterval %s is within localDataIntervals", dataInterval)
		pureData = true
	} else {
		log.Debugf("dataInterval %s is not within localDataIntervals", dataInterval)
	}
	if !pureData {
		// ask for data and wait for data, and then write data
		log.Debug("asking for data")
		err := session.receiveDataAndWriteFile(file, dataInterval)
		if err != nil {
			return err
		}
		log.Debug("got and written data")
	} else {
		// ask client to calculate checksum, calculate local checksum, then wait
		// for remote checksum, and then compare. If checksum matches, then send
		// continueSync reply. Otherwise, ask for data and wait for data, and then write data
		log.Debug("reply by asking for checksum of interval:", dataInterval)
		err := session.send(replyHeader{replyMagic, sendChecksum})
		if err != nil {
			log.Error("encode replyHeader error:", err)
			return err
		}
		localCheckSum, err := HashDataInterval(file, dataInterval)
		if err != nil {
			log.Errorf("HashDataInterval locally: %s failed, err: %s", dataInterval, err)
			return err
		}
		var checksum []byte
		err = session.receive(&checksum)
		if err != nil {
			log.Error("encode replyHeader error:", err)
			return err
		}
		log.Debug("got checksum:", checksum)
		if !bytes.Equal(localCheckSum, checksum) {
			log.Debug("checksum is not good")
			log.Debug("remote checksum:", checksum)
			log.Debug("local checksum:", localCheckSum)
			log.Debug("asking for data")
			err := session.receiveDataAndWriteFile(file, dataInterval)
			if err != nil {
				return err
			}
			log.Debug("got and written data")
		} else {
			log.Debug("checksum is good")
		}
	}

	log.Debug("asking for continueSync")
	err = session.send(replyHeader{replyMagic, continueSync})
	if err != nil {
		log.Error("encode replyHeader error:", err)
		return err
	}

	return nil
}

func (session *SyncSessionServer) receiveDataAndWriteFile(file FileIoProcessor, dataInterval Interval) error {
	// ask for data and wait for data, and then write data
	log.Debug("reply by asking for data of interval:", dataInterval)
	err := session.send(replyHeader{replyMagic, sendData})
	if err != nil {
		log.Error("encode replyHeader error:", err)
		return err
	}

	// create a byte slice to receive data
	var dataBuffer []byte
	err = session.receive(&dataBuffer)
	if err != nil {
		log.Error("decode dataBuffer error:", err)
		return err
	}
	log.Debugf("receiveDataAndWriteFile: got data byte count: %d", len(dataBuffer))
	log.Debug("needs to write to disk")

	// Write file with received data into the range
	err = WriteDataInterval(file, dataInterval, dataBuffer)
	if err != nil {
		return err
	}

	return nil
}

func getFileLayout(file FileIoProcessor) ([]Interval, []Interval, error) {
	fileSize, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Error("Failed to get size of the file:", err)
		return nil, nil, err
	}

	var holeIntervals []Interval
	var dataIntervals []Interval

	exts, err := GetFiemapExtents(file)
	if err != nil {
		return holeIntervals, dataIntervals, err
	}

	var lastIntervalEnd int64
	var holeInterval Interval

	// Process extents and create a layout with holes as well for easy syncing with client
	for index, e := range exts {
		interval := Interval{int64(e.Logical), int64(e.Logical + e.Length)}
		log.Debugf("Extent: %s, %x", interval, e.Flags)

		if lastIntervalEnd < interval.Begin {
			// report hole
			holeInterval = Interval{lastIntervalEnd, interval.Begin}
			log.Debugf("Here is a hole: %s", holeInterval)
			holeIntervals = append(holeIntervals, holeInterval)
		}
		// report data
		log.Debugf("Here is a data: %s", interval)
		lastIntervalEnd = interval.End
		dataIntervals = append(dataIntervals, interval)

		if e.Flags&FIEMAP_EXTENT_LAST != 0 {
			log.Debugf("hit the last extent with FIEMAP_EXTENT_LAST flag, are we on last index yet ? %v", (index == len(exts)-1))

			// report last hole
			if lastIntervalEnd < fileSize {
				holeInterval := Interval{lastIntervalEnd, fileSize}
				log.Debugf("Here is a hole: %s", holeInterval)
				holeIntervals = append(holeIntervals, holeInterval)
			}
		}
	}

	return holeIntervals, dataIntervals, nil
}
