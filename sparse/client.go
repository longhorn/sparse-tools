package sparse

import "net"
import "github.com/kp6/alphorn/log"
import "encoding/gob"
import "os"
import "strconv"

// TCPEndPoint tcp connection address
type TCPEndPoint struct {
	Host string
	Port int16
}

const connectionRetries = 5

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, addr TCPEndPoint, remotePath string) bool {
	file, err := os.Open(localPath)
	if nil != err {
		log.Error("Failed to open local source file:", localPath)
		return false
	}
	defer file.Close()

	size, errSize := file.Seek(0, os.SEEK_END)
	if nil != errSize {
		log.Error("Failed to get size of local source file:", localPath, errSize)
		return false
	}

	conn := connect(addr.Host, strconv.Itoa(int(addr.Port)))
	if nil == conn {
		log.Error("Failed to connect to", addr)
		return false
	}
	defer conn.Close()

	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	status := sendSyncRequest(encoder, decoder, remotePath, size)
	if !status {
		return false
	}

	localLayout := getLocalFileLayout(file)
	remoteLayout := getRemoteFileLayout(decoder)
	processDiff(encoder, decoder, localLayout, remoteLayout, file)
	return status
}

func connect(host, port string) net.Conn {
	// connect to this socket
	endpoint := host + ":" + port
	for retries := 1; retries <= connectionRetries; retries++ {
		conn, err := net.Dial("tcp", endpoint)
		if err == nil {
			return conn
		}
		log.Warn("Failed connection to", endpoint, "Retrying...")
	}
	return nil
}

func sendSyncRequest(encoder *gob.Encoder, decoder *gob.Decoder, path string, size int64) bool {
	err := encoder.Encode(requestHeader{requestMagic, syncRequestCode})
	if nil != err {
		log.Error("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(path)
	if nil != err {
		log.Error("Client protocol encoder error:", err)
		return false
	}
	err = encoder.Encode(size)
	if nil != err {
		log.Error("Client protocol encoder error:", err)
		return false
	}

	var ack bool
	err = decoder.Decode(&ack)
	if nil != err {
		log.Error("Client protocol decoder error:", err)
		return false
	}

	return ack
}

func getLocalFileLayout(file *os.File) []FileInterval {
	size, err := file.Seek(0, os.SEEK_END)
	if nil != err {
		log.Fatal("cannot retrieve local source file size", err)
	}
	return RetrieveLayout(file, Interval{0, size})
}

func getRemoteFileLayout(decoder *gob.Decoder) []FileInterval {
	var layout []FileInterval
	err := decoder.Decode(&layout)
	if nil != err {
		log.Fatal("Cient protocol error:", err)
	}
	log.Debug("Received layout:", layout)
	return layout
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

func processDiff(encoder *gob.Encoder, decoder *gob.Decoder, local, remote []FileInterval, file *os.File) bool {
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
			go fileReader(file, fileStream, netStream, fileStatus)
		} else {
			f, _ := os.Open(file.Name())
			go fileReader(f, fileStream, netStream, fileStatus)
		}
	}

	status := true
	for i, j := 0, 0; status && i < len(local); {
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
			log.Fatal("internal error")
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
	status = <-netStatus
	if !status {
		return false // network send or file read error
	}

	var statusRemote bool
	err := decoder.Decode(&statusRemote)
	if nil != err {
		log.Fatal("Cient protocol remote status error:", err)
	}
	return status && statusRemote
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

func networkSender(netStream <-chan diffChunk, encoder *gob.Encoder, netStatus chan<- bool) {
	status := true
	for {
		chunk := <-netStream
		if 0 == chunk.header.Len() {
			// eof: last 0 len header
			err := encoder.Encode(chunk.header)
			if nil != err {
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

		// Encode and send data to the network
		err := encoder.Encode(chunk.header)
		if nil != err {
			log.Error("Client protocol encoder error:", err)
			status = false
			continue
		}
        if nil == chunk.data {
            continue
        }
		err = encoder.Encode(chunk.data)
		if nil != err {
			log.Error("Client protocol encoder error:", err)
			status = false
			continue
		}
	}
	log.Info("Finished sending file diff, status =", status)
	netStatus <- status
}

func fileReader(file *os.File, fileStream <-chan fileChunk, netStream chan<- diffChunk, fileStatus chan<- bool) {
	for {
		chunk := <-fileStream
		if chunk.eof {
			break
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
		netStream <- diffChunk{status, r, data}
	}
	log.Info("Finished reading file")
	fileStatus <- true
}
