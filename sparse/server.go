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
	if nil != err {
		log.Fatal("Connection listener error:", err)
	}
    defer ln.Close()
	log.Info("Sync server is up...")
    
	for {
		conn, err := ln.Accept()
		if nil != err {
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
    requestMagic requestCode = 31415926
    syncRequestCode requestCode = 1
)

type requestHeader struct {
    Magic requestCode
    Code requestCode
}

func serveConnection(conn net.Conn) {
	defer conn.Close()
    
	decoder := gob.NewDecoder(conn)
    var request requestHeader
    err := decoder.Decode(&request)
    if nil != err {
        log.Error("Protocol decoder error:", err)
        return
    }
    if requestMagic!= request.Magic {
        log.Error("Bad request")
        return
    }
    
    switch request.Code {
    case syncRequestCode:
        var path string 
        err := decoder.Decode(&path)
        if nil != err {
            log.Error("Protocol decoder error:", err)
            return
        }
        serveSyncRequest(conn, path)
    }
}

func serveSyncRequest(conn net.Conn, path string)  {
    encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
    
    
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if nil != err {
		file, err = os.Create(path)
		if nil != err {
			log.Error("Failed to create file:", string(path))
			reply := "create file failure"
			// reply back to client
			conn.Write([]byte(reply + "\n"))
			return
		}
	}
	defer file.Close()

    encoder.Encode(true) // ACK request

	// load
	layout := loadFile(file)

	// send layout back
	items := len(layout)
	log.Info("Sending layout, item count=", items)
	err = encoder.Encode(layout)
	if nil != err {
		log.Error("Protocol encoder error:", err)
		return
	}

	// receive & process data diff
	status := true
	for status {
		var delta FileInterval
		err := decoder.Decode(&delta)
		if nil != err {
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
			if nil != err {
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
			if nil != err {
				log.Error("Failed to write file")
                status = false
                break
			}
		case SparseHole:
			log.Debug("trimming...")
			err := PunchHole(file, delta.Interval)
			if nil != err {
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
	if nil != err {
		log.Error("Protocol encoder error:", err)
        return
	}
}

func loadFile(file *os.File) []FileInterval {
	size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return make([]FileInterval, 0)
	}

	return RetrieveLayout(file, Interval{0, size})
}
