package sparse

import "net"
import "github.com/kp6/alphorn/log"
import "encoding/gob"
import "os"
import "errors"
import "strconv"
import "fmt"
import "time"

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

const connectionRetries = 5

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
	remoteLayout, err := getRemoteFileLayout(decoder)
	if err != nil {
		log.Error("Failed to retrieve remote file layout:", err)
		return err
	}
	return processDiff(encoder, decoder, localLayout, remoteLayout, file)
}

func connect(host, port string, timeout int) net.Conn {
	// connect to this socket
	endpoint := host + ":" + port
	raddr, err := net.ResolveTCPAddr("tcp", endpoint)
	if err != nil {
		log.Fatal("Connection address resolution error:", err)
	}
	timeStart := time.Now()
    timeStop := timeStart.Add(time.Duration(timeout)*time.Second)
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

func getRemoteFileLayout(decoder *gob.Decoder) ([]FileInterval, error) {
	var layout []FileInterval
	err := decoder.Decode(&layout)
	if err != nil {
		log.Fatal("Cient protocol error:", err)
		return nil, err
	}
	log.Debug("Received layout:", layout)
	return layout, nil
}

// file reading chunk
type fileChunk struct {
	eof    bool // end of stream: stop reader
	header FileInterval
}

// network transfer chunk
type diffChunk struct {
	status bool // read file or network send error yield false
	header FileInterval
	data   interface{}
}

func processDiff(encoder *gob.Encoder, decoder *gob.Decoder, local, remote []FileInterval, file *os.File) error {
	// Local:   __ _*
	// Remote:  *_ **
	const concurrentReaders = 4
	netStream := make(chan diffChunk, 128)
	netStatus := make(chan bool)
	go networkSender(netStream, encoder, netStatus)
	fileStream := make(chan fileChunk, 128)
	fileStatus := make(chan bool)
	for i := 0; i < concurrentReaders; i++ {
		if 0 == i {
			go fileReader(i, file, fileStream, netStream, fileStatus)
		} else {
			f, _ := os.Open(file.Name())
			go fileReader(i, f, fileStream, netStream, fileStatus)
		}
	}

	for i, j := 0, 0; i < len(local); {
		if j >= len(remote) {
			// Copy local tail
			processFileInterval(local[i], fileStream, netStream)
			i++
			continue
		}
		// Diff
		lrange := local[i]
		rrange := remote[j]
		if lrange.Begin == rrange.Begin {
			if lrange.End > rrange.End {
				local[i].End = rrange.End
				if SparseData == local[i].Kind || local[i].Kind != remote[j].Kind {
					processFileInterval(local[i], fileStream, netStream)
				}
				local[i].Begin = rrange.End
				local[i].End = lrange.End
				j++
				continue
			} else if lrange.End < rrange.End {
				if SparseData == local[i].Kind || local[i].Kind != remote[j].Kind {
					processFileInterval(local[i], fileStream, netStream)
				}
				remote[j].Begin = lrange.End
				i++
				continue
			}
			if SparseData == local[i].Kind || local[i].Kind != remote[j].Kind {
				processFileInterval(local[i], fileStream, netStream)
			}
			i++
			j++
		} else {
			// Should never happen
			log.Fatal("internal error")
			return errors.New("internal error")
		}
	}
	log.Info("Finished processing file diff")

	// stop file readers
	for i := 0; i < concurrentReaders; i++ {
		fileStream <- fileChunk{true, FileInterval{SparseHole, Interval{0, 0}}}
		<-fileStatus // wait for reader completion
	}
	// Send end of transmission
	netStream <- diffChunk{true, FileInterval{SparseHole, Interval{0, 0}}, nil}

	// get network sender status
	status := <-netStatus
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
	return nil
}

func processFileInterval(r FileInterval, fileStream chan<- fileChunk, netStream chan<- diffChunk) {
	const batch = 128 * Blocks

	// Process hole
	if SparseHole == r.Kind {
		netStream <- diffChunk{true, r, nil}
		return
	}

	// Process data in chunks
	for offset := r.Begin; offset < r.End; {
		size := batch
		if offset+size > r.End {
			size = r.End - offset
		}
		fileStream <- fileChunk{false, FileInterval{SparseData, Interval{offset, offset + size}}}
		offset += size
	}
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
			err := encoder.Encode(chunk.header)
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
		err := encoder.Encode(chunk.header)
		if err != nil {
			log.Error("Client protocol encoder error:", err)
			status = false
			continue
		}
		if nil == chunk.data {
			continue
		}
		err = encoder.Encode(chunk.data)
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
		netStream <- diffChunk{status, r, data}
	}
	log.Info("Finished reading file")
	fileStatus <- true
}
