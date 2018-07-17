package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"io/ioutil"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type CompactTask struct {
	partitionId uint32
	chunkId     int
	isLeader    bool
}

func (t *CompactTask) toString() (m string) {
	return fmt.Sprintf("dataPartition[%v]_chunk[%v]_isLeader[%v]", t.partitionId, t.chunkId, t.isLeader)
}

const (
	CompactThreadNum = 4
)

var (
	ErrDiskCompactChanFull = errors.New("disk compact chan is full")
)

var (
	// Regexp pattern for data partition dir name validate.
	RegexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
)

type Disk struct {
	sync.Mutex
	Path                            string
	ReadErrs                        uint64
	WriteErrs                       uint64
	All                             uint64
	Used                            uint64
	Available                       uint64
	PartitionCnt                    uint64
	RemainWeightsForCreatePartition uint64
	CreatedPartitionWeights         uint64
	MaxErrs                         int
	Status                          int
	PartitionNames                  []string
	RestSize                        uint64
	compactCh                       chan *CompactTask
	space                           SpaceManager
}

func NewDisk(path string, restSize uint64, maxErrs int) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.RestSize = restSize
	d.MaxErrs = maxErrs
	d.PartitionNames = make([]string, 0)
	d.RestSize = util.GB * 1
	d.MaxErrs = 2000
	d.compactCh = make(chan *CompactTask, CompactThreadNum)
	for i := 0; i < CompactThreadNum; i++ {
		go d.compact()
	}
	d.computeUsage()
	d.computePartitionCnt()

	d.startScheduleTasks()
	return
}

func (d *Disk) computeUsage() (err error) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}
	d.All = uint64(math.Max(float64(fs.Blocks*uint64(fs.Bsize))-float64(d.RestSize), 0))
	d.Available = uint64(math.Max(float64(fs.Bavail*uint64(fs.Bsize))-float64(d.RestSize), 0))
	d.Used = d.All - d.Available
	log.LogDebugf("action[computeUsage] disk[%v] all[%v] available[%v] used[%v]", d.Path, d.All, d.Available, d.Used)

	return
}

func (d *Disk) addTask(t *CompactTask) (err error) {
	select {
	case d.compactCh <- t:
		return
	default:
		return errors.Annotatef(ErrDiskCompactChanFull, "diskPath:[%v] partitionId[%v]", d.Path, t.partitionId)
	}
}

func (d *Disk) addReadErr() {
	atomic.AddUint64(&d.ReadErrs, 1)
}

func (d *Disk) compact() {
	for {
		select {
		case t := <-d.compactCh:
			dp := d.space.GetPartition(t.partitionId)
			if dp == nil {
				continue
			}
			err, release := dp.GetTinyStore().DoCompactWork(t.chunkId)
			if err != nil {
				log.LogErrorf("action[compact] task[%v] compact error[%v]", t.toString(), err.Error())
			} else {
				log.LogInfof("action[compact] task[%v] compact success Release [%v]", t.toString(), release)
			}
		}
	}
}

func (d *Disk) addWriteErr() {
	atomic.AddUint64(&d.WriteErrs, 1)
}

func (d *Disk) computePartitionCnt() {
	finfos, err := ioutil.ReadDir(d.Path)
	if err != nil {
		return
	}
	var count uint64
	dataPartitionSize := 0
	dataPartitionNames := make([]string, 0)
	for _, fInfo := range finfos {
		if fInfo.IsDir() && d.isPartitionDir(fInfo.Name()) {
			arr := strings.Split(fInfo.Name(), "_")
			if len(arr) != 3 {
				continue
			}
			_, dataPartitionSize, err = unmarshalPartitionName(fInfo.Name())
			if err != nil {
				continue
			}
			count += 1
			dataPartitionNames = append(dataPartitionNames, fInfo.Name())
		}
	}
	d.Lock()
	atomic.StoreUint64(&d.PartitionCnt, count)
	d.PartitionNames = dataPartitionNames
	d.RemainWeightsForCreatePartition = d.All - d.RestSize - uint64(len(d.PartitionNames)*dataPartitionSize)
	d.CreatedPartitionWeights = uint64(len(d.PartitionNames) * dataPartitionSize)
	d.Unlock()
}

func (d *Disk) startScheduleTasks() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				d.computeUsage()
				d.computePartitionCnt()
				d.updateSpaceInfo()
			}
		}
	}()
}

func (d *Disk) updateSpaceInfo() (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.addReadErr()
	}
	currErrs := d.ReadErrs + d.WriteErrs
	if currErrs >= uint64(d.MaxErrs) {
		d.Status = proto.Unavaliable
	} else if d.Available <= 0 {
		d.Status = proto.ReadOnly
	} else {
		d.Status = proto.ReadWrite
	}
	msg := fmt.Sprintf("action[updateSpaceInfo] disk[%v] total[%v] realAvail[%v] partitionsAvail[%v]"+
		"minRestSize[%v] maxErrs[%v] readErrs[%v] writeErrs[%v] partitionStatus[%v]", d.Path,
		d.All, d.Available, d.RemainWeightsForCreatePartition, d.RestSize, d.MaxErrs, d.ReadErrs, d.WriteErrs, d.Status)
	log.LogDebugf(msg)

	return
}

func (d *Disk) AddDataPartition(dp *dataPartition) {
	name := dp.String()
	d.Lock()
	defer d.Unlock()
	d.PartitionNames = append(d.PartitionNames, name)
	atomic.AddUint64(&d.PartitionCnt, 1)
	d.RemainWeightsForCreatePartition = d.All - d.RestSize - uint64(len(d.PartitionNames)*dp.Size())
	d.CreatedPartitionWeights += uint64(dp.partitionSize)
}

func (d *Disk) DataPartitionList() (partitionIds []uint32) {
	d.Lock()
	defer d.Unlock()
	partitionIds = make([]uint32, 0)
	for _, name := range d.PartitionNames {
		vid, _, err := unmarshalPartitionName(name)
		if err != nil {
			continue
		}
		partitionIds = append(partitionIds, vid)
	}
	return
}

func unmarshalPartitionName(name string) (partitionId uint32, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error dataPartition name[%v]", name)
		return
	}
	var (
		pId int
	)
	if pId, err = strconv.Atoi(arr[1]); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	partitionId = uint32(pId)
	return
}

func (d *Disk) isPartitionDir(filename string) (is bool) {
	is = RegexpDataPartitionDir.MatchString(filename)
	return
}

func (d *Disk) RestorePartition(space SpaceManager) {
	var (
		partitionId   uint32
		partitionSize int
	)
	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[RestorePartition] read dir[%v] err[%v].", d.Path, err)
		return
	}
	var wg sync.WaitGroup
	for _, fileInfo := range fileInfoList {
		filename := fileInfo.Name()
		if !d.isPartitionDir(filename) {
			continue
		}

		if partitionId, partitionSize, err = unmarshalPartitionName(filename); err != nil {
			log.LogErrorf("action[RestorePartition] unmarshal partitionName[%v] from disk[%v] err[%v] ",
				filename, d.Path, err.Error())
			continue
		}
		log.LogDebugf("acton[RestorePartition] disk info path[%v] name[%v] partitionId[%v] partitionSize[%v].",
			d.Path, fileInfo.Name(), partitionId, partitionSize)
		wg.Add(1)
		go func(partitionId uint32,filename string) {
			var (
				dp            DataPartition
				err           error
			)
			defer wg.Done()
			if dp, err = LoadDataPartition(path.Join(d.Path, filename), d); err != nil {
				log.LogError(fmt.Sprintf("action[RestorePartition] new partition[%v] err[%v] ",
					partitionId, err.Error()))
				return
			}
			if space.GetPartition(partitionId) == nil {
				space.PutPartition(dp)
				log.LogDebugf("action[RestorePartition] put partition[%v] to space manager.", dp.ID())
			}
		}(partitionId,filename)
	}
	wg.Wait()
}
