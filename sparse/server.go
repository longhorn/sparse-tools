package sparse

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"hash"
	"net"
	"os"
	"strconv"
	"time"
	"bytes"

	"github.com/rancher/sparse-tools/log"
)

// Server daemon
func Server(addr TCPEndPoint, timeout int) {
	server(addr, true /*serve single connection for now*/, timeout)
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(addr TCPEndPoint, timeout int) {
	server(addr, true, timeout)
}

const fileReaders = 1
const fileWriters = 1
const verboseServer = true

func server(addr TCPEndPoint, serveOnce /*test flag*/ bool, timeout int) {
	serverConnectionTimeout := time.Duration(timeout) * time.Second
	// listen on all interfaces
	EndPoint := addr.Host + ":" + strconv.Itoa(int(addr.Port))
	laddr, err := net.ResolveTCPAddr("tcp", EndPoint)
	if err != nil {
		log.Fatal("Connection listener address resolution error:", err)
	}
	ln, err := net.ListenTCP("tcp", laddr)
	if err != nil {
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

type requestCode int

const (
	requestMagic    requestCode = 31415926
	syncRequestCode requestCode = 1 + iota
	syncHole
	syncData
	syncDone
)

type requestHeader struct {
	Magic requestCode
	Code  requestCode
}

type replyCode int

const (
	replyMagic    replyCode = 31415928
	continueSync  replyCode = 1 + iota
	sendChecksum
	sendData
)

type replyHeader struct {
	Magic replyCode
	Code  replyCode
}

// returns true if no retry is necessary
func serveConnection(conn net.Conn) bool {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	var request requestHeader
	err := decoder.Decode(&request)
	if err != nil {
		log.Fatal("Protocol decoder error:", err)
		return true
	}
	if requestMagic != request.Magic {
		log.Error("Bad request")
		return true
	}

	switch request.Code {
	case syncRequestCode:
		var path string
		err := decoder.Decode(&path)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return true
		}
		var size int64
		err = decoder.Decode(&size)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return true
		}
		var salt []byte
		err = decoder.Decode(&salt)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return true
		}
		encoder := gob.NewEncoder(conn)
		return serveSyncRequest(encoder, decoder, path, size, salt)
	}
	return true
}

// returns true if no retry is necessary
func serveSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64, salt []byte) bool {
	directFileIO := size%Blocks == 0
	SetupFileIO(directFileIO)

	// Open destination file
	file, err := fileOpen(path, os.O_RDWR, 0666)
	if err != nil {
		file, err = os.Create(path)
		if err != nil {
			log.Error("Failed to create file:", string(path), err)
			encoder.Encode(false) // NACK request
			return true
		}
	}
	// Setup close sequence
	if directFileIO {
		defer file.Sync()
	}
	defer file.Close()

	// Resize the file
	if err = file.Truncate(size); err != nil {
		log.Error("Failed to resize file:", string(path), err)
		encoder.Encode(false) // NACK request
		return true
	}

	// open file
	fileRO, err := fileOpen(path, os.O_RDONLY, 0)
	if err != nil {
		log.Error("Failed to open file for reading:", string(path), err)
		encoder.Encode(false) // NACK request
		return true
	}
	defer fileRO.Close()

	// the starting point of interval sync request within originalFileIntervalLayout slice
	index := 0
	originalFileIntervalLayout, err := getOriginalFileLayout(file)
	if err != nil {
		log.Error("Failed to getOriginalFileLayout", err)
		encoder.Encode(false) // NACK request
		return true
	}

	// loop for getting request until all synced
	var request requestHeader
	moreRequest := true
	for moreRequest && index < len(originalFileIntervalLayout) {
		err := decoder.Decode(&request)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			return true
		}

		switch request.Code {
		case syncDone:
			moreRequest = false

		case syncHole:
			/*
			sync hole interval:

			1. get the hole range(start and end byte offsets)
			2. ensure the range is within pure hole extents. If not, it will punch hole for the entire range
			3. it will ask for continue
			*/
			var holeInterval Interval
			err := decoder.Decode(&holeInterval)
			if err != nil {
				log.Fatal("Protocol decoder error:", err)
				status = false
				break
			}
			log.Debug("receiving hole interval: ", holeInterval)

			pureHole := true
			var current FileInterval
			for {
				// loop until found an interval end larger than or equal to the sync hole end
				// note: the beginning should always be the same 
				current = originalFileIntervalLayout[index]
				if current.Kind != SparseHole {
					pureHole := false
				}
				if current.End >= holeInterval.End {
					break
				}
				index++
			}

			if current.End == holeInterval.End {
				index++
			} else {
				// adjust next starting FileInterval
				current.Begin = holeInterval.End
			}
			if !pureHole {
				// TODO punch hole for the entire range from synHole
				log.Debug("punching hole:", holeInterval)
			}
			
			// send continue reply
			encoder.Encode(replyHeader{replyMagic, continueSync})

		case syncData:
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
			err := decoder.Decode(&dataInterval)
			if err != nil {
				log.Fatal("Protocol decoder error:", err)
				status = false
				break
			}
			log.Debug("receiving data interval: ", dataInterval)

			pureData := true
			var current FileInterval
			for {
				// loop until found an interval end larger than or equal to the sync data end
				// note: the beginning should always be the same due to adjustment below
				current = originalFileIntervalLayout[index]
				if current.Kind != SparseData {
					pureData := false
				}
				if current.End >= dataInterval.End {
					break
				}
				index++
			}

			if current.End == dataInterval.End {
				index++
			} else {
				// adjust next starting FileInterval
				current.Begin = dataInterval.End
			}
			if !pureData {
				// ask for data and wait for data, and then write data
				receiveDataBytesAndWrite(encoder, decoder, dataInterval)
			} else {
				// ask client to calculate checksum, calculate local checksum, then wait
				// for remote checksum, and then compare. If checksum matches, then send
				// continueSync reply. Otherwise, ask for data and wait for data, and then write data
				log.Debug("reply by asking for checksum of interval:", dataInterval)
				encoder.Encode(replyHeader{replyMagic, sendChecksum})

				// TODO calculate local checksum
				localCheckSum := make([]byte, 20, 20)

				// create a byte slice to receive checksum
				checksum := make([]byte, 0, 20)
				decoder.Decode(&checksum)

				if !bytes.Equal(localCheckSum, checksum) {
					receiveDataBytesAndWrite(encoder, decoder, dataInterval)
				}
			}
			
			// send continue reply
			encoder.Encode(replyHeader{replyMagic, continueSync})
		}
	}

	return true
}


func receiveDataBytesAndWrite(encoder *gob.Encoder, decoder *gob.Decoder, dataInterval Interval) {
	// ask for data and wait for data, and then write data
	log.Debug("reply by asking for data of interval:", dataInterval)
	encoder.Encode(replyHeader{replyMagic, sendData})

	// create a byte slice to receive data
	dataBuffer := make([]byte, 0, dataInterval.Len())
	decoder.Decode(&dataBuffer)

	if len(dataBuffer) == dataInterval.Len() {
		log.Info("got the correct amount of data bytes, needs to write to disk")
		// TODO Write file with received data into the range
	}
}


func getOriginalFileLayout(file *os.File) ([]FileInterval, error) {
	layOutFileIntervals := make([]FileInterval, 0)
	fiemap := NewFiemapFile(file)

	// first call of Fiemap with 0 extent count will actually return total mapped ext counts
	// we can use that to allocate extent struct slice to get details of each extent
	extCount, _, errno := fiemap.Fiemap(0)
	if errno != nil {
		log.Fatal("failed to call fiemap.Fiemap(0)")
		return layOutFileIntervals, fmt.Errorf(errno.Error())
	} else {
		log.Infof("extCount: %d", extCount)
	}
	_, exts, errno := fiemap.Fiemap(extCount)
	if errno != nil {
		log.Fatal("failed to call fiemap.Fiemap(extCount)")
		return layOutFileIntervals, fmt.Errorf(errno.Error())
	} else {
		log.Infof("got extents[]: %d", len(exts))
	}

	var lastIntervalEnd int64
	var holeInterval Interval
	
	// Process extents and create a layout with holes as well for easy syncing with client
	for index, e := range exts {
		interval := Interval{int64(e.Logical), int64(e.Logical + e.Length)}
		log.Printf("Extent: %s, %x", interval, e.Flags)

		if lastIntervalEnd < interval.Begin {
			// report hole
			holeInterval = Interval{lastIntervalEnd, interval.Begin}
			log.Printf("Here is a hole: %s", holeInterval)
			layOutFileIntervals = append(layOutFileIntervals, FileInterval{SparseHole, holeInterval})
		}
		// report data
		log.Printf("Here is a data: %s", interval)
		lastIntervalEnd = interval.End
		layOutFileIntervals = append(layOutFileIntervals, FileInterval{SparseData, interval})

		if e.Flags & FIEMAP_EXTENT_LAST != 0 {
			log.Printf("hit the last extent with FIEMAP_EXTENT_LAST flag, are we on last index yet ? %v", (index == len(exts) - 1))

			// report last hole
			if lastIntervalEnd < fileSize {
				holeInterval := Interval{lastIntervalEnd, fileSize }
				log.Printf("Here is a hole: %s", holeInterval)
				layOutFileIntervals = append(layOutFileIntervals, FileInterval{SparseHole, holeInterval})
			}
		}
	}

	return layOutFileIntervals, nil
}

// Tee ordered intervals into the network and checksum checker
func Tee(orderedStream <-chan HashedDataInterval, netOutStream chan<- HashedInterval, checksumStream chan<- DataInterval) {
	for r := range orderedStream {
		netOutStream <- HashedInterval{r.FileInterval, r.Hash}
		checksumStream <- DataInterval{r.FileInterval, r.Data}
	}
	close(netOutStream)
	close(checksumStream)
}

func netSender(netOutStream <-chan HashedInterval, encoder *gob.Encoder, netOutDoneStream chan<- bool) {
	for r := range netOutStream {
		if verboseServer {
			log.Debug("Server.netSender: sending", r.FileInterval)
		}
		err := encoder.Encode(r)
		if err != nil {
			log.Fatal("Protocol encoder error:", err)
			netOutDoneStream <- false
			return
		}
	}

	rEOF := HashedInterval{FileInterval{SparseIgnore, Interval{}}, make([]byte, 0)}
	if rEOF.Len() != 0 {
		log.Fatal("Server.netSender internal error")
	}
	// err := encoder.Encode(HashedInterval{FileInterval{}, make([]byte, 0)})
	err := encoder.Encode(rEOF)
	if err != nil {
		log.Fatal("Protocol encoder error:", err)
		netOutDoneStream <- false
		return
	}
	if verboseServer {
		log.Debug("Server.netSender: finished sending hashes")
	}
	netOutDoneStream <- true
}

func netReceiver(decoder *gob.Decoder, file *os.File, netInStream chan<- DataInterval, fileStream chan<- DataInterval, deltaReceiverDone chan<- bool) {
	// receive & process data diff
	status := true
	for status {
		var delta FileInterval
		err := decoder.Decode(&delta)
		if err != nil {
			log.Fatal("Protocol decoder error:", err)
			status = false
			break
		}
		log.Debug("receiving delta [", delta, "]")
		if 0 == delta.Len() {
			log.Debug("received end of transimission marker")
			break // end of diff
		}
		switch delta.Kind {
		case SparseData:
			// Receive data
			var data []byte
			err = decoder.Decode(&data)
			if err != nil {
				log.Fatal("Protocol data decoder error:", err)
				status = false
				break
			}
			if int64(len(data)) != delta.Len() {
				log.Fatal("Failed to receive data, expected=", delta.Len(), "received=", len(data))
				status = false
				break
			}
			// Push for writing and vaildator processing
			fileStream <- DataInterval{delta, data}
			netInStream <- DataInterval{delta, data}

		case SparseHole:
			// Push for writing and vaildator processing
			fileStream <- DataInterval{delta, make([]byte, 0)}
			netInStream <- DataInterval{delta, make([]byte, 0)}

		case SparseIgnore:
			// Push for vaildator processing
			netInStream <- DataInterval{delta, make([]byte, 0)}
			log.Debug("ignoring...")
		}
	}

	log.Debug("Server.netReceiver done, sync")
	close(netInStream)
	close(fileStream)
	deltaReceiverDone <- status
}

func logData(prefix string, data []byte) {
	size := len(data)
	if size > 0 {
		log.Debug("\t", prefix, "of", size, "bytes", data[0], "...")
	} else {
		log.Debug("\t", prefix, "of", size, "bytes")
	}
}

func hashFileData(fileHasher hash.Hash, dataLen int64, data []byte) {
	// Hash hole length or data if any
	if len(data) == 0 {
		// hash hole
		hole := make([]byte, 8)
		binary.PutVarint(hole, dataLen)
		fileHasher.Write(hole)

	} else {
		fileHasher.Write(data)
	}
}

// Validator merges source and diff data; produces hash of the destination file
func Validator(salt []byte, checksumStream, netInStream <-chan DataInterval, resultStream chan<- []byte) {
	fileHasher := sha1.New()
	//TODO: error handling
	fileHasher.Write(salt)
	r := <-checksumStream // original dst file data
	q := <-netInStream    // diff data
	for q.Len() != 0 || r.Len() != 0 {
		if r.Len() == 0 /*end of dst file*/ {
			// Hash diff data
			if verboseServer {
				logData("RHASH", q.Data)
			}
			hashFileData(fileHasher, q.Len(), q.Data)
			q = <-netInStream
		} else if q.Len() == 0 /*end of diff file*/ {
			// Hash original data
			if verboseServer {
				logData("RHASH", r.Data)
			}
			hashFileData(fileHasher, r.Len(), r.Data)
			r = <-checksumStream
		} else {
			qi := q.Interval
			ri := r.Interval
			if qi.Begin == ri.Begin {
				if qi.End > ri.End {
					log.Fatal("Server.Validator internal error, diff=", q.FileInterval, "local=", r.FileInterval)
				} else if qi.End < ri.End {
					// Hash diff data
					if verboseServer {
						log.Debug("Server.Validator: hashing diff", q.FileInterval, r.FileInterval)
					}
					if verboseServer {
						logData("RHASH", q.Data)
					}
					hashFileData(fileHasher, q.Len(), q.Data)
					r.Begin = q.End
					q = <-netInStream
				} else {
					if q.Kind == SparseIgnore {
						// Hash original data
						if verboseServer {
							log.Debug("Server.Validator: hashing original", r.FileInterval)
						}
						if verboseServer {
							logData("RHASH", r.Data)
						}
						hashFileData(fileHasher, r.Len(), r.Data)
					} else {
						// Hash diff data
						if verboseServer {
							log.Debug("Server.Validator: hashing diff", q.FileInterval)
						}
						if verboseServer {
							logData("RHASH", q.Data)
						}
						hashFileData(fileHasher, q.Len(), q.Data)
					}
					q = <-netInStream
					r = <-checksumStream
				}
			} else {
				log.Fatal("Server.Validator internal error, diff=", q.FileInterval, "local=", r.FileInterval)
			}
		}
	}

	if verboseServer {
		log.Debug("Server.Validator: finished")
	}
	resultStream <- fileHasher.Sum(nil)
}
