package sparse

import "net"
import "github.com/kp6/alphorn/log"
import "os"
import "encoding/gob"
import "strconv"

// Server daemon
func Server(addr TCPEndPoint) {
	server(addr, false)
}

// TestServer daemon serves only one connection for each test then exits
func TestServer(addr TCPEndPoint) {
	server(addr, true)
}

func server(addr TCPEndPoint, serveOnce /*test flag*/ bool) {
	// listen on all interfaces
	EndPoint := addr.Host + ":" + strconv.Itoa(int(addr.Port))
	ln, err := net.Listen("tcp", EndPoint) // accept connection on port
	if err != nil {
		log.Fatal("Connection listener error:", err)
	}
	defer ln.Close()
	log.Info("Sync server is up...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Connection accept error:", err)
		}
		go serveConnection(conn)

		if serveOnce {
			// This is to avoid server listening port conflicts while running tests
			// exit after single connection request
			break
		}
	}
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
	layout, err := loadFile(file)
    if err != nil {
		encoder.Encode(false) // NACK request
        return
    }
	encoder.Encode(true) // ACK request

	// send layout back
	items := len(layout)
	log.Info("Sending layout, item count=", items)
	err = encoder.Encode(layout)
	if err != nil {
		log.Error("Protocol encoder error:", err)
		return
	}

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
			log.Debug("writing data...")
			_, err = file.WriteAt(data, delta.Begin)
			if err != nil {
				log.Error("Failed to write file")
				status = false
				break
			}
		case SparseHole:
			log.Debug("trimming...")
			err := PunchHole(file, delta.Interval)
			if err != nil {
				log.Error("Failed to trim file")
				status = false
				break
			}
		}
	}

	file.Sync() //TODO: switch to O_DIRECT and compare performance

	// reply to client with status
	log.Info("Sync remote status=", status)
	err = encoder.Encode(status)
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
