package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

type SyncFileOperations interface {
	UpdateSyncFileProgress(size int64)
}

type SyncFileStub struct{}

func (f *SyncFileStub) UpdateSyncFileProgress(size int64) {}

func (server *SyncServer) getQueryDirectIO(request *http.Request) (bool, error) {
	queryParams := request.URL.Query()
	directIOStr := queryParams.Get("directIO")
	if directIOStr == "" {
		return false, fmt.Errorf("directIO does not exist in the queries %+v", queryParams)
	}

	directIO, err := strconv.ParseBool(directIOStr)
	if err != nil {
		return false, errors.Wrapf(err, "failed to parse directIO string %v", directIOStr)
	}
	return directIO, nil
}

func (server *SyncServer) getQueryInterval(request *http.Request) (sparse.Interval, error) {
	var interval sparse.Interval

	queryParams := request.URL.Query()

	beginStr := queryParams.Get("begin") // only one value for key begin
	endStr := queryParams.Get("end")     // only one value for key end
	if beginStr == "" || endStr == "" {
		return interval, fmt.Errorf("queryParams begin or end does not exist")
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		return interval, errors.Wrapf(err, "failed to parse begin string %v", begin)
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return interval, errors.Wrapf(err, "failed to parse end string %v", end)
	}

	return sparse.Interval{Begin: begin, End: end}, nil
}

func (server *SyncServer) open(writer http.ResponseWriter, request *http.Request) {
	err := server.doOpen(request)
	if err != nil {
		log.WithError(err).Error("Failed to open Ssync server")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Info("Ssync server opened and ready")
}

func (server *SyncServer) doOpen(request *http.Request) error {
	// get directIO
	directIO, err := server.getQueryDirectIO(request)
	if err != nil {
		return errors.Wrap(err, "failed to query directIO")
	}

	// get file size
	interval, err := server.getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	if directIO && interval.End%sparse.Blocks != 0 {
		return fmt.Errorf("invalid file size %v for directIO", interval.End)
	}
	log.Infof("Receiving %v: size %v, directIO %v", server.filePath, interval.End, directIO)

	fileIo, err := server.getFileIo(directIO)
	if err != nil {
		return errors.Wrapf(err, "failed to open/create local source file %v", server.filePath)
	}

	err = fileIo.Truncate(interval.End)
	if err != nil {
		return errors.Wrapf(err, "failed to truncate local source file %v", server.filePath)
	}

	// initialize the server file object
	server.fileIo = fileIo

	return nil
}

func (server *SyncServer) close(writer http.ResponseWriter, request *http.Request) {
	if f, ok := writer.(http.Flusher); ok {
		f.Flush()
	}

	if server.fileIo != nil {
		server.fileIo.Close()
	}
	log.Infof("Closing ssync server")

	log.Infof("Closing Ssync server")
	server.cancelFunc()
}

func (server *SyncServer) sendHole(writer http.ResponseWriter, request *http.Request) {
	err := server.doSendHole(request)
	if err != nil {
		log.WithError(err).Error("Failed to send hole")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doSendHole(request *http.Request) error {
	remoteHoleInterval, err := server.getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	fiemap := sparse.NewFiemapFile(server.fileIo.GetFile())
	err = fiemap.PunchHole(remoteHoleInterval.Begin, remoteHoleInterval.Len())
	if err != nil {
		return errors.Wrapf(err, "failed to punch hole interval %+v", remoteHoleInterval)
	}

	return nil
}

func (server *SyncServer) getChecksum(writer http.ResponseWriter, request *http.Request) {
	err := server.doGetChecksum(writer, request)
	if err != nil {
		log.WithError(err).Error("Failed to get checksum")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doGetChecksum(writer http.ResponseWriter, request *http.Request) error {
	remoteDataInterval, err := server.getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	var checksum []byte

	// For the region to have valid data, it can only has one extent covering the whole region
	exts, err := sparse.GetFiemapRegionExts(server.fileIo, remoteDataInterval, 2)
	if len(exts) == 1 && int64(exts[0].Logical) <= remoteDataInterval.Begin &&
		int64(exts[0].Logical+exts[0].Length) >= remoteDataInterval.End {

		checksum, err = sparse.HashFileInterval(server.fileIo, remoteDataInterval)
		if err != nil {
			return errors.Wrapf(err, "failed to hash interval %+v", remoteDataInterval)
		}
	}

	outgoingJSON, err := json.Marshal(checksum)
	if err != nil {
		return errors.Wrap(err, "failed to marshal checksum")
	}

	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprint(writer, string(outgoingJSON))
	server.syncFileOps.UpdateSyncFileProgress(remoteDataInterval.Len())

	return nil
}

func (server *SyncServer) writeData(writer http.ResponseWriter, request *http.Request) {
	err := server.doWriteData(request)
	if err != nil {
		log.WithError(err).Error("Failed to write data")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doWriteData(request *http.Request) error {
	remoteDataInterval, err := server.getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	log.Tracef("writeData: interval %+v", remoteDataInterval)

	data, err := ioutil.ReadAll(io.LimitReader(request.Body, remoteDataInterval.End-remoteDataInterval.Begin))
	if err != nil {
		return errors.Wrap(err, "failed to read request")
	}

	// Write file with received data into the range
	err = sparse.WriteDataInterval(server.fileIo, remoteDataInterval, data)
	if err != nil {
		return errors.Wrapf(err, "failed to write data interval %+v", remoteDataInterval)
	}

	return nil
}
