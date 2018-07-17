package main

import (
	"flag"
	"os"
	"fmt"
	"io"
	"hash/crc32"
)

const (
	BlockCrcHeaderSize  = PerBlockCrcSize*BlockCount+1
	BlockCount          = 1024*2
	MarkDelete          = 'D'
	UnMarkDelete        = 'U'
	MarkDeleteIndex     = BlockCrcHeaderSize-1
	BlockSize           = 65536 * 2
	PerBlockCrcSize     = 4
	DeleteIndexFileName = "delete.index"
	ChunkOpenOpt      = os.O_CREATE | os.O_RDWR | os.O_APPEND
)

var (
	name=flag.String("name","f","read file name")
)

func main()  {
	flag.Parse()
	f,err:=os.Open(*name)
	if err!=nil {
		fmt.Println(err)
		return
	}
	var offset int64
	offset=BlockCrcHeaderSize
	if err!=nil {
		fmt.Println(err)
		return
	}
	exist :=false
	blockNo:=0
	for {
		data:=make([]byte,BlockSize)
		n,err:=f.ReadAt(data,offset)
		if err==io.EOF{
			exist =true
		}
		crc:=crc32.ChecksumIEEE(data[:n])
		fmt.Println(fmt.Sprintf("blockNO[%v] crc[%v] n[%v]",blockNo,crc,n))
		offset+=int64(n)
		blockNo+=1
		if exist {
			break
		}
	}
	f.Close()
}
