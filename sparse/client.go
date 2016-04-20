package sparse

import (
	"crypto/sha1"
	"net"
	"os"
	"strconv"

    "github.com/rancher/sparse-tools/log"
)

import "encoding/gob"

import "errors"

import "fmt"
import "time"

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

const connectionRetries = 5
const verboseClient = true

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, addr TCPEndPoint, remotePath string, timeout int) error {
	file, err := os.Open(localPath)
	if err != nil {
		log.Error("Failed to open local source file:", localPath)
		return err
	}
	defer file.Close()

	size, errSize := file.Seek(0, os.SEEK_END)
	if errSize != nil {
		log.Error("Failed to get size of local source file:", localPath, errSize)
		return err
	}

	conn := connect(addr.Host, strconv.Itoa(int(addr.Port)), timeout)
	if nil == conn {
		log.Error("Failed to connect to", addr)
		return err
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	status := sendSyncRequest(encoder, decoder, remotePath, size)
	if !status {
		return err
	}

	localLayout, err := getLocalFileLayout(file)
	if err != nil {
		log.Error("Failed to retrieve local file layout:", err)
		return err
	}
	splitterStream := make(chan FileInterval, 128)
	fileStream := make(chan FileInterval, 128)
	unorderedStream := make(chan HashedDataInterval, 128)
	orderedStream := make(chan HashedDataInterval, 128)
	go IntervalSplitter(splitterStream, fileStream)
	go FileReader(fileStream, file, unorderedStream)
	go OrderIntervals(unorderedStream, orderedStream)

	// Get remote file intervals and their hashes
	netInStream := make(chan HashedInterval, 128)
	netInStreamDone := make(chan bool)
	go netDstReceiver(decoder, netInStream, netInStreamDone)

	for _, interval := range localLayout {
		splitterStream <- interval
	}
	close(splitterStream)

	return processDiff(encoder, decoder, orderedStream, netInStream, netInStreamDone, file)
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
			return conn
		}
		log.Warn("Failed connection to", endpoint, "Retrying...")
		time.Sleep(1 * time.Second)
	}
	return nil
}

func sendSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) bool {
	err := encoder.Encode(requestHeader{requestMagic, syncRequestCode})
	if err != nil {
		log.Error("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(path)
	if err != nil {
		log.Error("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(size)
	if err != nil {
		log.Error("Client protocol encoder error:", err)
		return false
	}

	var ack bool
	err = decoder.Decode(&ack)
	if err != nil {
		log.Error("Client protocol decoder error:", err)
		return false
	}

	return ack
}

func getLocalFileLayout(file *os.File) ([]FileInterval, error) {
	size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("cannot retrieve local source file size", err)
		return nil, err
	}
	return RetrieveLayout(file, Interval{0, size})
}

// Get remote hashed intervals
func netDstReceiver(decoder *gob.Decoder, netInStream chan<- HashedInterval, netInStreamDone chan<- bool) {
	var r HashedInterval
	status := true
	for {
		if verboseClient {
			log.Debug("Client.netDstReceiver decoding...")
		}
		err := decoder.Decode(&r)
		if err != nil {
			log.Fatal("Cient protocol error:", err)
			status = false
			break
		}
		// interval := r.Interval
		if r.Kind == SparseIgnore {
			if verboseClient {
				log.Debug("Client.netDstReceiver got <eof>")
			}
			break
		}
		if verboseClient {
			switch r.Kind {
			case SparseData:
				log.Debug("Client.netDstReceiver got data", r.FileInterval, "hash[", len(r.Hash), "]")
			case SparseHole:
				log.Debug("Client.netDstReceiver got hole", r.FileInterval)
			}
		}
		netInStream <- r
	}
	close(netInStream)
	netInStreamDone <- status
}

// file reading chunk
type fileChunk struct {
	eof    bool // end of stream: stop reader
	header FileInterval
}

// network transfer chunk
type diffChunk struct {
	status bool // read file or network send error yield false
	header DataInterval
}

func processDiff(encoder *gob.Encoder, decoder *gob.Decoder, local <-chan HashedDataInterval, remote <-chan HashedInterval, netInStreamDone <-chan bool, file *os.File) error {
	// Local:   __ _*
	// Remote:  *_ **
	const concurrentReaders = 4
	netStream := make(chan diffChunk, 128)
	netStatus := make(chan bool)
	go networkSender(netStream, encoder, netStatus)
	// fileStream := make(chan fileChunk, 128)
	// fileStatus := make(chan bool)
	// for i := 0; i < concurrentReaders; i++ {
	// 	if 0 == i {
	// 		go fileReader(i, file, fileStream, netStream, fileStatus)
	// 	} else {
	// 		f, _ := os.Open(file.Name())
	// 		go fileReader(i, f, fileStream, netStream, fileStatus)
	// 	}
	// }
	fileHasher := sha1.New()
	fileHasher.Write(HashSalt)

	lrange := <-local
	rrange := <-remote
	for lrange.Len() != 0 {
		if rrange.Len() == 0 {
			// Copy local tail
			if verboseClient {
				logData("LHASH", lrange.Data)
			}
			fileHasher.Write(lrange.Data)
			processFileInterval(lrange, HashedInterval{FileInterval{SparseHole, lrange.Interval}, make([]byte, 0)}, netStream)
			lrange = <-local
			continue
		}
		// Diff
		if verboseClient {
			log.Debug("Diff:", lrange.FileInterval, rrange.FileInterval)
		}
		if lrange.Begin == rrange.Begin {
			if lrange.End > rrange.End {
				unprocessed := lrange.End
				lrange.End = rrange.End
				if verboseClient {
					logData("LHASH", lrange.Data[:lrange.Len()])
				}
				fileHasher.Write(lrange.Data[:lrange.Len()])
				processFileInterval(lrange, rrange, netStream)
				lrange.Begin = rrange.End
				lrange.End = unprocessed
				rrange = <-remote
				continue
			} else if lrange.End < rrange.End {
				if verboseClient {
					logData("LHASH", lrange.Data)
				}
				fileHasher.Write(lrange.Data)
				processFileInterval(lrange, HashedInterval{FileInterval{rrange.Kind, lrange.Interval}, make([]byte, 0)}, netStream)
				rrange.Begin = lrange.End
				lrange = <-local
				continue
			}
			if verboseClient {
				logData("LHASH", lrange.Data)
			}
			fileHasher.Write(lrange.Data)
			processFileInterval(lrange, rrange, netStream)
			lrange = <-local
			rrange = <-remote
		} else {
			// Should never happen
			log.Fatal("internal error")
			return errors.New("internal error")
		}
	}
	log.Info("Finished processing file diff")

	// // stop file readers
	// for i := 0; i < concurrentReaders; i++ {
	// 	fileStream <- fileChunk{true, FileInterval{SparseHole, Interval{0, 0}}}
	// 	<-fileStatus // wait for reader completion
	// }

	// make sure we finished consuming dst hashes
	status := <-netInStreamDone // netDstReceiver finished
	log.Info("Finished consuming remote file hashes")

	// Send end of transmission
	netStream <- diffChunk{true, DataInterval{FileInterval{SparseIgnore, Interval{0, 0}}, make([]byte, 0)}}

	// get network sender status
	status = <-netStatus
	if !status {
		return errors.New("netwoek transfer failure")
	}

	var statusRemote bool
	err := decoder.Decode(&statusRemote)
	if err != nil {
		log.Fatal("Cient protocol remote status error:", err)
		return err
	}
	if !statusRemote {
		return errors.New("failure on remote sync site")
	}
	var hashRemote []byte
	err = decoder.Decode(&hashRemote)
	if err != nil {
		log.Fatal("Cient protocol remote hash error:", err)
		return err
	}
	hashLocal := fileHasher.Sum(nil)
	log.Info("hashLocal=", hashLocal)
	log.Info("hashRemote=", hashRemote)
	if isHashDifferent(hashLocal, hashRemote) {
		return errors.New("file hash divergence")
	}
	return nil
}

func isHashDifferent(a, b []byte) bool {
	if len(a) != len(b) {
		return true
	}
	for i, val := range a {
		if val != b[i] {
			return true
		}
	}
	return false // hashes are equal
}

func processFileInterval(local HashedDataInterval, remote HashedInterval, netStream chan<- diffChunk) {
	if local.Kind != remote.Kind {
		// Different intreval types, send the diff
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	// The intervals types are the same
	if SparseHole == local.Kind {
		// Process hole, no syncronization is required
		local.Kind = SparseIgnore
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	// Data file interval
	if isHashDifferent(local.Hash, remote.Hash) {
		netStream <- diffChunk{true, DataInterval{local.FileInterval, local.Data}}
		return
	}

	// No diff, just communicate we processed it
	//TODO: this apparently can be avoided but requires revision of the protocol
	local.Kind = SparseIgnore
	netStream <- diffChunk{true, DataInterval{local.FileInterval, make([]byte, 0)}}
}

// prints chan codes and lengths to trace
// - sequence and interleaving of chan processing
// - how much of the chan buffer is used
const traceChannelLoad = false

func networkSender(netStream <-chan diffChunk, encoder *gob.Encoder, netStatus chan<- bool) {
	status := true
	for {
		chunk := <-netStream
		if 0 == chunk.header.Len() {
			// eof: last 0 len header
			if verboseClient {
				log.Debug("Client.networkSender <eof>")
			}
			err := encoder.Encode(chunk.header.FileInterval)
			if err != nil {
				log.Error("Client protocol encoder error:", err)
				status = false
			}
			break
		}

		if !status {
			// network error
			continue // discard the chunk
		}
		if !chunk.status {
			// read error
			status = false
			continue // discard the chunk
		}

		if traceChannelLoad {
			fmt.Fprint(os.Stderr, len(netStream), "n")
		}

		// Encode and send data to the network
		if verboseClient {
			log.Debug("Client.networkSender sending:", chunk.header.FileInterval)
		}
		err := encoder.Encode(chunk.header.FileInterval)
		if err != nil {
			log.Error("Client protocol encoder error:", err)
			status = false
			continue
		}
		if len(chunk.header.Data) == 0 {
			continue
		}
		if verboseClient {
			log.Debug("Client.networkSender sending data")
		}
		err = encoder.Encode(chunk.header.Data)
		if err != nil {
			log.Error("Client protocol encoder error:", err)
			status = false
			continue
		}
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, "N\n")
		}
	}
	log.Info("Finished sending file diff, status =", status)
	netStatus <- status
}

// obsolete method
func fileReader(id int, file *os.File, fileStream <-chan fileChunk, netStream chan<- diffChunk, fileStatus chan<- bool) {
	idBeg := map[int]string{0: "a", 1: "b", 2: "c", 3: "d"}
	idEnd := map[int]string{0: "A", 1: "B", 2: "C", 3: "D"}
	for {
		chunk := <-fileStream
		if chunk.eof {
			break
		}
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, len(fileStream), idBeg[id])
		}
		// Check interval type
		r := chunk.header
		if SparseData != r.Kind {
			log.Fatal("internal error: noles should be send directly to netStream")
		}

		// Read file data
		data := make([]byte, r.Len())
		status := true
		n, err := file.ReadAt(data, r.Begin)
		if err != nil {
			log.Error("File read error")
			status = false
		} else if int64(n) != r.Len() {
			log.Error("File read underrun")
			status = false
		}

		// Send file data
		if traceChannelLoad {
			fmt.Fprint(os.Stderr, idEnd[id])
		}
		netStream <- diffChunk{status, DataInterval{r, data}}
	}
	log.Info("Finished reading file")
	fileStatus <- true
}
