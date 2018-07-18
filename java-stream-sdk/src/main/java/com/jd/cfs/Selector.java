package com.jd.cfs;



import com.jd.cfs.dataserver.Volume;
import com.jd.cfs.dataserver.DataPartition;

import java.io.IOException;
import java.util.Set;


public interface Selector {

    /**
     * 从指定的volume中选取一个DataPartition来进行写操作
     *
     * @param volName     指定的volName
     * @param excludeDataPartition 需要排除的DataPartition 集合
     * @return 返回可以写的DataPartition, 若为null则没有DataPartition可写
     */
    DataPartition selectForWrite(String volName, Set<DataPartition> excludeDataPartition) throws IOException;


    /**
     * 根据指定的volName，partitionId选择对应的DataPartition
     *
     * @param volName 指定的User
     * @param partitionId     指定的partitionId
     * @return 返回Key所对应的volume 若为空，则没有volume可读
     */
    DataPartition selectForRead(String volName, int partitionId) throws IOException;


    Volume getVolume(String volName) throws IOException;

}
