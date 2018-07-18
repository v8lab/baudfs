package com.jd.cfs.dataserver;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.*;

import static com.jd.cfs.common.Constants.MIN_WRITEABLE_DATA_PARTITIONS;


public class Volume {
    @JsonProperty("DataPartitions")
    private List<DataPartition> dataPartitions;

    private Set<DataPartition> writable;

    Volume() {
        this.dataPartitions = new ArrayList<>();
    }

    public List<DataPartition> getDataPartitions() {
        return this.dataPartitions;
    }

    private void setDataPartitions(List<DataPartition> dataPartitions) {
        this.dataPartitions = new ArrayList<>();
        this.dataPartitions.addAll(dataPartitions);
    }

    void sort() {
        Collections.sort(dataPartitions, new Comparator<DataPartition>() {
            @Override
            public int compare(DataPartition o1, DataPartition o2) {
                return o1.getPartitionId().compareTo(o2.getPartitionId());
            }
        });
        dataPartitions = ImmutableList.copyOf(dataPartitions);
    }

    public DataPartition getVolume(int volId) {
        int pos = Collections.binarySearch(dataPartitions, volId);
        if (pos < 0) return null;
        return dataPartitions.get(pos);
    }

    public Set<DataPartition> getWritable() {
        if (writable == null) {
            Iterable<DataPartition> result = Iterables.filter(dataPartitions, new Predicate<DataPartition>() {
                @Override
                public boolean apply(DataPartition input) {
                    return input.getState() == DataPartition.State.WRITEABLE;
                }
            });
            writable = Sets.newCopyOnWriteArraySet(result);
        }
        return writable;
    }

    void resetDataPartitions(Volume oldVolume) {
        if (oldVolume.getDataPartitions() == null) {
            return;
        }

        if (this.getDataPartitions() == null) {
            this.setDataPartitions(oldVolume.getDataPartitions());
        }

        Set<DataPartition> writable = this.getWritable();

        if (writable.size() < MIN_WRITEABLE_DATA_PARTITIONS) {
            this.setDataPartitions(oldVolume.getDataPartitions());
        }

        if (oldVolume.getDataPartitions().size() > this.getDataPartitions().size()) {
            this.setDataPartitions(oldVolume.getDataPartitions());
        }

    }

}
