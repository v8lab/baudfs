package meta

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/util/btree"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	HostsSeparator       = ","
	MetaPartitionViewURL = "/client/vol"
	GetVolStatURL        = "/client/volStat"
	GetClusterInfoURL    = "/admin/getIp"

	RefreshMetaPartitionsInterval = time.Minute * 5
)

const (
	statusUnknown int = iota
	statusOK
	statusExist
	statusNoent
	statusFull
	statusAgain
	statusError
)

type MetaWrapper struct {
	sync.RWMutex
	cluster string
	volname string
	master  util.MasterHelper
	conns   *pool.ConnPool

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	totalSize uint64
	usedSize  uint64
}

func NewMetaWrapper(volname, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.volname = volname
	master := strings.Split(masterHosts, HostsSeparator)
	mw.master = util.NewMasterHelper()
	for _, ip := range master {
		mw.master.AddNode(ip)
	}
	mw.conns = pool.NewConnPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.UpdateClusterInfo()
	mw.UpdateVolStatInfo()
	if err := mw.UpdateMetaPartitions(); err != nil {
		return nil, err
	}
	go mw.refresh()
	return mw, nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

func (mw *MetaWrapper) umpKey(act string) string {
	return fmt.Sprintf("%s_sdk_meta_%s", mw.cluster, act)
}

// Proto ResultCode to status
func parseStatus(result uint8) (status int) {
	switch result {
	case proto.OpOk:
		status = statusOK
	case proto.OpExistErr:
		status = statusExist
	case proto.OpNotExistErr:
		status = statusNoent
	case proto.OpInodeFullErr:
		status = statusFull
	case proto.OpAgain:
		status = statusAgain
	default:
		status = statusError
	}
	return
}
