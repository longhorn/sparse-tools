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

	status := sendSyncRequest(conn, remotePath, size)
	if !status {
		return false
	}

	localLayout := getLocalFileLayout(file)
	remoteLayout := getRemoteFileLayout(conn)
	processDiff(localLayout, remoteLayout, file, conn)
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

func sendSyncRequest(conn net.Conn, path string, size int64) bool {
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)

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
	if nil !=err {
		log.Fatal("cannot retrieve local source file size", err)
	}
	return RetrieveLayout(file, Interval{0, size})
}

func getRemoteFileLayout(conn net.Conn) []FileInterval {
	decoder := gob.NewDecoder(conn)
	var layout []FileInterval
	err := decoder.Decode(&layout)
	if nil != err {
		log.Fatal("Cient protocol error:", err)
	}
	log.Debug("Received layout:", layout)
	return layout
}

func processDiff(local, remote []FileInterval, file *os.File, conn net.Conn) bool {
	// Local:   __ _*
	// Remote:  *_ **
	encoder := gob.NewEncoder(conn)
	status := true
	for i, j := 0, 0; status && i < len(local); {
		if j >= len(remote) {
			// Copy local tail
			sendFileData(local[i], file, encoder)
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
					status = sendFileData(local[i], file, encoder)
				}
				local[i].Begin = rrange.End
				local[i].End = lrange.End
				j++
				continue
			} else if lrange.End < rrange.End {
				if SparseData == local[i].Kind || local[i].Kind != remote[j].Kind {
					status = sendFileData(local[i], file, encoder)
				}
				remote[j].Begin = lrange.End
				i++
				continue
			}
			if SparseData == local[i].Kind || local[i].Kind != remote[j].Kind {
				status = sendFileData(local[i], file, encoder)
			}
			i++
			j++
		} else {
			log.Fatal("internal error")
			return false
		}
	}

	// end of transmission
	if status {
		status = sendFileData(FileInterval{SparseHole, Interval{0, 0}}, file, encoder)
	}
    var statusRemote bool;
	decoder := gob.NewDecoder(conn)
	err := decoder.Decode(&statusRemote)
	if nil != err {
		log.Fatal("Cient protocol remote status error:", err)
	}
	return status && statusRemote
}

func sendFileData(r FileInterval, file *os.File, encoder *gob.Encoder) bool {
	const batch = 128 * Blocks

	// Send end of transmission
	if 0 == r.Len() {
		err := encoder.Encode(r)
		if nil != err {
			log.Error("Client protocol encoder error:", err)
			return false
		}
		return true
	}

	// Send hole
	if SparseHole == r.Kind {
		err := encoder.Encode(r)
		if nil != err {
			log.Error("Client protocol encoder error:", err)
			return false
		}
		return true
	}

	// Send data
	for offset := r.Begin; offset < r.End; {
		size := batch
		if offset+size > r.End {
			size = r.End - offset
		}
		err := encoder.Encode(FileInterval{SparseData, Interval{offset, offset + size}})
		if nil != err {
			log.Error("Client protocol encoder error:", err)
			return false
		}
		data := make([]byte, size)
		n, err := file.ReadAt(data, offset)
		if err != nil {
			log.Error("File read error")
			return false
		}
		if int64(n) != size {
			log.Error("File read underrun")
			return false
		}
		err = encoder.Encode(data)
        if nil != err {
            log.Error("Client protocol data encoder error:", err)
            return false
        }
		offset += size
	}
	return true
}
