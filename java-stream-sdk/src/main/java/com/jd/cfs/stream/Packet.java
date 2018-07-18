package com.jd.cfs.stream;


import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.message.Message;
import com.jd.cfs.message.Protocol;
import com.jd.cfs.utils.PureJavaCrc32;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;


class Packet {
    private ByteBuffer buffer;
    private int dataStart;
    private Checksum checksum = new PureJavaCrc32();
    private int offsetInExtent;
    int serialnumber;

    Packet(DataPartition partition, long fid, int serialnumber, int offset) {
        String dataNode = partition.replication();
        this.offsetInExtent = offset;
        dataStart = Protocol.HEAD_SIZE + dataNode.length();
        buffer = ByteBuffer.allocate(dataStart + Protocol.EXTENT_PACKET_SIZE);
        buffer.put(Protocol.MAGIC);
        buffer.put(Protocol.EXTENT);
        buffer.put(Protocol.Command.Write.code);
        buffer.put((byte) 0);
        buffer.put((byte) (partition.getServers().size()-1));
        buffer.putInt(0);   //crc
        buffer.putInt(0);   //size
        buffer.putInt(dataNode.length());//arglen
        buffer.putInt(partition.getPartitionId());
        buffer.putLong(fid);
        buffer.putLong(offset);
        buffer.putLong(Message.id.getAndIncrement());
        buffer.put(dataNode.getBytes());
        this.serialnumber = serialnumber;
    }

    int fill(byte[] data, int offset, int len) {
        int canWrite = Math.min(buffer.remaining(), len);
        buffer.put(data, offset, canWrite);
        checksum.update(data, offset, canWrite);
        return canWrite;
    }

    void writeTo(OutputStream os) throws IOException {
        int crc32 = (int) checksum.getValue();
        buffer.putInt(Protocol.CRC_INDEX, crc32); //reset crc
        buffer.putInt(Protocol.SIZE_INDEX, getDataLen());//reset size
        os.write(buffer.array(), 0, buffer.position());
    }

    byte[] getData() {
        byte[] data = new byte[buffer.position() - dataStart];
        System.arraycopy(buffer.array(), dataStart, data, 0, data.length);
        return data;
    }

    int getDataLen() {
        return buffer.position() - dataStart;
    }

    long getRequestId() {
        return buffer.getLong(Protocol.REQUEST_ID_INDEX);
    }

    @Override
    public String toString() {
        return "Packet{SN=" + serialnumber + ",Offset=" + offsetInExtent +
                ",Length=" + this.getDataLen() + ",RequestId=" + this.getRequestId() + "}";
    }
}