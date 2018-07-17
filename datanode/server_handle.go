package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"net/http"
	"strconv"
)

func (s *DataNode) handleGetDisk(w http.ResponseWriter, r *http.Request) {
	diskReport := &struct{
		Disks []*Disk
		Rack string
	}{
		Disks: s.space.GetDisks(),
		Rack:  s.rackName,
	}
	s.buildApiSuccessResp(w, diskReport)
}

func (s *DataNode) handleStat(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartBeatResponse{}
	s.fillHeartBeatResponse(response)
	s.buildApiSuccessResp(w, response)
}

func (s *DataNode) apiGetPartitions(w http.ResponseWriter, r *http.Request) {
	partitions := make([]interface{}, 0)
	s.space.RangePartitions(func(dp DataPartition) bool {
		partition := &struct {
			ID       uint32
			Size     int
			Used     int
			Status   int
			Path     string
			Replicas []string
		}{
			ID:       dp.ID(),
			Size:     dp.Size(),
			Used:     dp.Used(),
			Status:   dp.Status(),
			Path:     dp.Path(),
			Replicas: dp.ReplicaHosts(),
		}
		partitions = append(partitions, partition)
		return true
	})
	result := &struct {
		Partitions     []interface{}
		PartitionCount int
	}{
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	s.buildApiSuccessResp(w, result)
}

func (s *DataNode) apiGetPartition(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionId = "id"
	)
	var (
		partitionId uint64
		files       []*storage.FileInfo
		err         error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionId, err = strconv.ParseUint(r.FormValue(paramPartitionId), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionId, err)
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.GetPartition(uint32(partitionId))
	if partition == nil {
		s.buildApiFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if files, err = partition.GetAllWaterMarker(); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildApiFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	result := &struct {
		ID        uint32
		Size      int
		Used      int
		Status    int
		Path      string
		Files     []*storage.FileInfo
		FileCount int
		Replicas  []string
	}{
		ID:        partition.ID(),
		Size:      partition.Size(),
		Used:      partition.Used(),
		Status:    partition.Status(),
		Path:      partition.Path(),
		Files:     files,
		FileCount: len(files),
		Replicas:  partition.ReplicaHosts(),
	}
	s.buildApiSuccessResp(w, result)
}

func (s *DataNode) handleExtentInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionId int
		extentId    int
		err         error
		extentInfo  *storage.FileInfo
	)
	if err = r.ParseForm(); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionId, err = strconv.Atoi(r.FormValue("partition")); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentId, err = strconv.Atoi(r.FormValue("extent")); err != nil {
		s.buildApiFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.GetPartition(uint32(partitionId))
	if partition == nil {
		s.buildApiFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.GetExtentStore().GetWatermark(uint64(extentId)); err != nil {
		s.buildApiFailureResp(w, 500, err.Error())
		return
	}

	s.buildApiSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) buildApiSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildApiJsonResp(w, http.StatusOK, data, "")
}

func (s *DataNode) buildApiFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildApiJsonResp(w, code, nil, msg)
}

func (s *DataNode) buildApiJsonResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"Code"`
		Data interface{} `json:"Data"`
		Msg  string      `json:"Msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}
