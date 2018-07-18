## 部署
# 编译
   - rocksdb版本要求：5.9.2
   - 下载 gorocksdb
      > git clone https://github.com/tecbot/gorocksdb.git
      > git checkout -b baudfs 3e47615
   - 假设rocksdb安装在默认目录，即可执行如下命令编译
      >CGO_CFLAGS="-I/usr/local/include" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz   -lbz2 -lsnappy " go build
# 编辑配置文件
  > 配置文件支持json格式，请参考示例，示例中的配置项都必须配置。示例：

  ```json
   {
  "role": "master",
  "ip": "127.0.0.1",
  "port": "8080",
  "prof":"10088",
  "id":"1",
  "peers": "1:127.0.0.1:8080,1:127.0.0.1:8081,1:127.0.0.1:8082",
  "logDir": "/export/master",
  "logLevel":"DEBUG",
  "retainLogs":"2000",
  "walDir":"/export/raft",
  "storeDir":"/export/rocks",
  "clusterName":"baudStorage"
}
```

# 启动
    >  nohup ./master -c config.json > nohup.out &

### 管理API参考

## 集群概况

    > http://127.0.0.1/admin/getCluster

## vol API

# 创建
    > http://127.0.0.1/admin/createVol?name=fs&replicas=3&type=extent
# 查看
    > http://127.0.0.1/client/vol?name=fs
# stat
    > http://127.0.0.1/client/volStat?name=fs

## MetaPartition API

# 查看
> http://127.0.0.1/client/metaPartition?name=fs&id=1

# 下线某个副本
> http://127.0.0.1/metaPartition/offline?name=fs&id=13&addr=ip:port

## dataPartition API

# 创建
> http://127.0.0.1/dataPartition/create?count=40&name=fs&type=extent
# 查看
> http://127.0.0.1/dataPartition/get?id=100
# 比对
> http://127.0.0.1/dataPartition/load?name=fs&id=1
# 下线某个副本
> http://127.0.0.1/dataPartition/offline?name=fs&id=13&addr=ip:port
# 查看某个Vol下所有的dataParitions
> http://127.0.0.1/client/dataPartitions?name=fs

## metaNode API

    > http://127.0.0.1/metaNode/get?addr=ip:port
    > http://127.0.0.1/metaNode/add?addr=ip:port
    > http://127.0.0.1/metaNode/offline?addr=ip:port

## dataNode API

    > http://127.0.0.1/dataNode/get?addr=ip:port
    > http://127.0.0.1/dataNode/add?addr=ip:port
    > http://127.0.0.1/dataNode/offline?addr=ip:port

## Master管理 API

# 添加节点
> http://127.0.0.1/raftNode/add?addr=ip:port&id=3
# 删除节点
> http://127.0.0.1/raftNode/remove?addr=ip:port&id=3