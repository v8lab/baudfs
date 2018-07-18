package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"hash/crc32"
	"net"
	"sync"
)

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *dataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairExtent(remoteExtentInfo)
	if err != nil {
		localExtentInfo, opErr := dp.GetExtentStore().GetWatermark(uint64(remoteExtentInfo.FileId))
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition[%v] remote[%v] local[%v]",
			dp.partitionId, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err[%v].", err)
	}
}

//extent file repair function,do it on follower host
func (dp *dataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetExtentStore()
	if !store.IsExistExtent(uint64(remoteExtentInfo.FileId)) {
		return
	}

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.FileId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// Get need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := NewStreamReadPacket(dp.ID(), remoteExtentInfo.FileId, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// Get a connection to leader host
	conn, err = gConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.Put(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err[%v].", err)
		return
	}
	for {
		// Get local extentFile size
		var (
			localExtentInfo *storage.FileInfo
		)
		if localExtentInfo, err = store.GetWatermark(uint64(remoteExtentInfo.FileId)); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent partition[%v] GetWatermark error", dp.partitionId)
			log.LogErrorf("action[streamRepairExtent] err[%v].", err)
			return
		}
		// If local extent size has great remoteExtent file size ,then break
		if localExtentInfo.Size >= remoteExtentInfo.Size {
			break
		}

		// Read 64k stream repair packet
		if err = request.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogError("action[streamRepairExtent] err[%v].", err)
			return
		}
		log.LogInfof("action[streamRepairExtent] partition[%v] extent[%v] start fix from [%v]"+
			" remoteSize[%v] localSize[%v].", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, localExtentInfo.Size)

		if request.Crc != crc32.ChecksumIEEE(request.Data[:request.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition[%v] extent[%v] start fix from [%v]"+
				" remoteSize[%v] localSize[%v] request[%v]", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, localExtentInfo.Size, request.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err[%v].", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if err = store.Write(uint64(localExtentInfo.FileId), int64(localExtentInfo.Size), int64(request.Size), request.Data, request.Crc); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err[%v].", err)
			return
		}
	}
	return

}
