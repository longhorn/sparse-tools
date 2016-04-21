package sparse

import "net"
import "github.com/rancher/sparse-tools/log"
import "os"
import "encoding/gob"
import "strconv"
import "time"
import "crypto/sha1"
import "hash"
import "encoding/binary"

// Server daemon
func Server(addr TCPEndPoint, timeout int) {
	server(addr, true /*serve single connection for now*/, timeout)
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(addr TCPEndPoint, timeout int) {
	server(addr, true, timeout)
}

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
			serveConnection(conn)
			break
		}

		go serveConnection(conn)
	}
	log.Info("Sync server exit.")
}

type requestCode int

const (
	requestMagic    requestCode = 31415926
	syncRequestCode requestCode = 1
)

type requestHeader struct {
	Magic requestCode
	Code  requestCode
}

func serveConnection(conn net.Conn) {
	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	var request requestHeader
	err := decoder.Decode(&request)
	if err != nil {
		log.Error("Protocol decoder error:", err)
		return
	}
	if requestMagic != request.Magic {
		log.Error("Bad request")
		return
	}

	switch request.Code {
	case syncRequestCode:
		var path string
		err := decoder.Decode(&path)
		if err != nil {
			log.Error("Protocol decoder error:", err)
			return
		}
		var size int64
		err = decoder.Decode(&size)
		if err != nil {
			log.Error("Protocol decoder error:", err)
			return
		}
		encoder := gob.NewEncoder(conn)
		serveSyncRequest(encoder, decoder, path, size)
	}
}

func serveSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) {

	// Open destination file
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		file, err = os.Create(path)
		if err != nil {
			log.Error("Failed to create file:", string(path), err)
			encoder.Encode(false) // NACK request
			return
		}
	}
	defer file.Close()

	// Resize the file
	if err = file.Truncate(size); err != nil {
		log.Error("Failed to resize file:", string(path), err)
		encoder.Encode(false) // NACK request
		return
	}

	// load
	fileRO, err := os.Open(path)
	if err != nil {
		log.Error("Failed to open file for reading:", string(path), err)
		encoder.Encode(false) // NACK request
		return
	}
	defer fileRO.Close()
	layout, err := loadFile(fileRO)
	if err != nil {
		encoder.Encode(false) // NACK request
		return
	}
	encoder.Encode(true) // ACK request

	splitterStream := make(chan FileInterval, 128)
	fileStream := make(chan FileInterval, 128)
	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)
	netOutStream := make(chan HashedInterval, 128)
	netOutDoneStream := make(chan bool)

	netInStream := make(chan DataInterval, 128)
	fileWrittenStreamDone := make(chan bool)
	checksumStream := make(chan DataInterval, 128)
	resultStream := make(chan []byte)

	go IntervalSplitter(splitterStream, fileStream)
	go FileReader(fileStream, fileRO, unorderedStream)
	go OrderIntervals(unorderedStream, orderedStream)
	go Tee(orderedStream, netOutStream, checksumStream)
	// Sends layout along with data hashes
	go netSender(netOutStream, encoder, netOutDoneStream)
	go netReceiver(decoder, file, netInStream, fileWrittenStreamDone) // receiver and checker
	go Validator(checksumStream, netInStream, resultStream)

	for _, interval := range layout {
		if verboseServer {
			log.Debug("Server file interval:", interval)
		}
		splitterStream <- interval
	}
	close(splitterStream)

	// Block till completion
	status := <-netOutDoneStream               // done sending dst hashes
	status = <-fileWrittenStreamDone && status // done writing dst file
	hash := <-resultStream                     // done processing diffs

	// reply to client with status
	log.Info("Sync server remote status=", status)
	err = encoder.Encode(status)
	if err != nil {
		log.Error("Protocol encoder error:", err)
		return
	}
	// reply with local hash
	err = encoder.Encode(hash)
	if err != nil {
		log.Error("Protocol encoder error:", err)
		return
	}
}

func loadFile(file *os.File) ([]FileInterval, error) {
	size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return make([]FileInterval, 0), err
	}

	return RetrieveLayout(file, Interval{0, size})
}

// IntervalSplitter limits file intervals to predefined batch size
func IntervalSplitter(spltterStream <-chan FileInterval, fileStream chan<- FileInterval) {
	const batch = 128 * Blocks
	for r := range spltterStream {
		switch r.Kind {
		case SparseHole:
			// Process hole
			fileStream <- r
		case SparseData:
			// Process data in chunks
			for offset := r.Begin; offset < r.End; {
				size := batch
				if offset+size > r.End {
					size = r.End - offset
				}
				fileStream <- FileInterval{SparseData, Interval{offset, offset + size}}
				offset += size
			}
		}
	}
	close(fileStream)
}

// HashedInterval FileInterval plus its data hash (to be sent to the client)
type HashedInterval struct {
	FileInterval
	Hash []byte
}

// HashedDataInterval FileInterval plus its hash and data
type HashedDataInterval struct {
	HashedInterval
	Data []byte
}

// DataInterval FileInterval plus its data
type DataInterval struct {
	FileInterval
	Data []byte
}

// HashSalt is common client/server hash salt
var HashSalt = []byte("TODO: randomize and exchange between client/server")

// FileReader supports concurrent file reading
func FileReader(fileStream <-chan FileInterval, file *os.File, unorderedStream chan<- HashedDataInterval) {
	for r := range fileStream {
		switch r.Kind {
		case SparseHole:
			// Process hole
			// hash := sha1.New()
			// binary.PutVariant(data, r.Len)
			// fileHash.Write(data)
			var hash, data []byte
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}

		case SparseData:
			// Read file data
			data := make([]byte, r.Len())
			status := true
			n, err := file.ReadAt(data, r.Begin)
			if err != nil {
				status = false
				log.Error("File read error", status)
			} else if int64(n) != r.Len() {
				status = false
				log.Error("File read underrun")
			}
			hasher := sha1.New()
			hasher.Write(HashSalt)
			hasher.Write(data)
			hash := hasher.Sum(nil)
			unorderedStream <- HashedDataInterval{HashedInterval{r, hash}, data}
		}
	}
	close(unorderedStream)
}

// OrderIntervals puts back "out of order" read results
func OrderIntervals(unorderedStream <-chan HashedDataInterval, orderedStream chan<- HashedDataInterval) {
	pos := int64(0)
	var m map[int64]HashedDataInterval // out of order completions
	for r := range unorderedStream {
		// Handle "in order" range
		if pos == r.Begin {
			orderedStream <- r
			pos = r.End
			continue
		}

		// push "out of order"" range
		m[r.Begin] = r

		// check the "out of order" stash for "in order"
		for pop, existsNext := m[pos]; existsNext; {
			// pop in order range
			orderedStream <- pop
			delete(m, pos)
			pos = pop.End
		}
	}
	close(orderedStream)
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
			log.Error("Protocol encoder error:", err)
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
		log.Error("Protocol encoder error:", err)
		netOutDoneStream <- false
		return
	}
	if verboseServer {
		log.Debug("Server.netSender: finished sending hashes")
	}
	netOutDoneStream <- true
}

func netReceiver(decoder *gob.Decoder, file *os.File, netInStream chan<- DataInterval, fileWrittenStreamDone chan<- bool) {
	// receive & process data diff
	status := true
	for status {
		var delta FileInterval
		err := decoder.Decode(&delta)
		if err != nil {
			log.Error("Protocol decoder error:", err)
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
				log.Error("Protocol data decoder error:", err)
				status = false
				break
			}
			if int64(len(data)) != delta.Len() {
				log.Error("Failed to receive data")
				status = false
				break
			}
			// Push for vaildator processing
			netInStream <- DataInterval{delta, data}

			log.Debug("writing data...")
			_, err = file.WriteAt(data, delta.Begin)
			if err != nil {
				log.Error("Failed to write file")
				status = false
				break
			}
		case SparseHole:
			// Push for vaildator processing
			netInStream <- DataInterval{delta, make([]byte, 0)}

			log.Debug("trimming...")
			err := PunchHole(file, delta.Interval)
			if err != nil {
				log.Error("Failed to trim file")
				status = false
				break
			}
		case SparseIgnore:
			// Push for vaildator processing
			netInStream <- DataInterval{delta, make([]byte, 0)}
			log.Debug("ignoring...")
		}
	}

	log.Debug("Server.netReceiver done, sync")
	file.Sync() //TODO: switch to O_DIRECT and compare performance
	close(netInStream)
	fileWrittenStreamDone <- status
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
func Validator(checksumStream, netInStream <-chan DataInterval, resultStream chan<- []byte) {
	fileHasher := sha1.New()
	//TODO: error handling
	fileHasher.Write(HashSalt)
	r := <-checksumStream // original dst file data
	for q := range netInStream {
		if r.Len() == 0 /*end of dst file*/ {
			// Hash diff data
			if verboseServer {
				logData("RHASH", q.Data)
			}
			hashFileData(fileHasher, q.Len(), q.Data)
		} else {
			qi := q.Interval
			ri := r.Interval
			if qi == ri {
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
				r = <-checksumStream // original dst file data
			} else {
				if qi.Len() < ri.Len() {
					// Hash diff data
					if verboseServer {
						log.Debug("Server.Validator: hashing diff", q.FileInterval)
					}
					if verboseServer {
						logData("RHASH", q.Data)
					}
					hashFileData(fileHasher, q.Len(), q.Data)
				} else {
					log.Fatal("Server.Validator internal error")
				}
			}
		}
	}

	if verboseServer {
		log.Debug("Server.Validator: finished")
	}
	resultStream <- fileHasher.Sum(nil)
}
