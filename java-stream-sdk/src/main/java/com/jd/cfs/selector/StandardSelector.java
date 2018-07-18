package com.jd.cfs.selector;


import com.google.common.collect.Sets;
import com.jd.cfs.Selector;
import com.jd.cfs.dataserver.Volume;
import com.jd.cfs.dataserver.VolumeHolder;
import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.exception.TooManyExcludeException;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

public class StandardSelector implements Selector {
    private VolumeHolder volumeHolder;
    private Random random = new Random();

    public StandardSelector(String master) {
        this.volumeHolder = new VolumeHolder(master);
    }

    @Override
    public DataPartition selectForWrite(String volName, final Set<DataPartition> excludeDataPartition) throws IOException {
        Set<DataPartition> writeable = getVolume(volName).getWritable();
        if (writeable.isEmpty()) {
            throw new IOException("ReadOnlyFilesystem: " + volName);
        }
        int count = writeable.size() - excludeDataPartition.size();
        if (count == 0) {
            throw new TooManyExcludeException();
        }
        Sets.SetView<DataPartition> available = Sets.difference(writeable, excludeDataPartition);
        int temp = random.nextInt(count);
        int i = 0;
        for (DataPartition v : available) {
            if (i == temp){
                return v;
            }
            i++;
        }
        return null;
    }

    @Override
    public DataPartition selectForRead(String volName, int partitionId) throws IOException {
        Volume volume = getVolume(volName);
        return volume.getVolume(partitionId);
    }

    public Volume getVolume(String volName) throws IOException {
        Volume volume = volumeHolder.getVolume(volName);
        if (volume.getDataPartitions().size() == 0) {
            throw new IOException("UnavailableFilesystem:No DataPartitions");
        }
        return volume;
    }
}
