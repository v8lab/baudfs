package com.jd.cfs;

/**
 * Created by zhuhongyin
 * Created on 2018/6/28.
 */
public class Key {
    private String vol;
    private int partitionId;
    private long extentId;
    private int size;
    private int crc;

    private long offset;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getVol() {
        return vol;
    }

    public void setVol(String vol) {
        this.vol = vol;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public long getExtentId() {
        return extentId;
    }

    public void setExtentId(long extentId) {
        this.extentId = extentId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public String toString() {
        return String.format("/jfs/b/%s/%d/%d/%d/%d", vol, partitionId, extentId, size, crc);
    }

    public Key clone() {
        Key key = new Key();
        key.vol = this.vol;
        key.partitionId = this.partitionId;
        key.extentId = this.extentId;
        key.offset = this.offset;
        key.size = this.size;
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Key key = (Key) o;

        return crc == key.crc && size == key.size && extentId == key.extentId &&
                partitionId == key.partitionId && vol.equals(key.vol);
    }

    @Override
    public int hashCode() {
        int result = vol.hashCode();
        result = 31 * result + partitionId;
        result = 31 * result + (int) (extentId ^ (extentId >>> 32));
        result = 31 * result + size;
        result = 31 * result + crc;
        return result;
    }
}
