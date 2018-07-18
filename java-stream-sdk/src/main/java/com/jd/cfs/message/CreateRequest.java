package com.jd.cfs.message;

import com.jd.cfs.dataserver.DataPartition;

import java.nio.ByteBuffer;

public class CreateRequest extends Message {
    private String hosts;


    public CreateRequest(DataPartition partition) {
        ByteBuffer buffer = ByteBuffer.wrap(head);
        hosts = partition.replication();
        buffer.put(Protocol.MAGIC);
        buffer.put(Protocol.EXTENT);
        buffer.put(Protocol.Command.Create.code);
        buffer.put((byte)0);//result code
        buffer.put((byte) (partition.getServers().size() - 1)); //nodes length
        buffer.putInt(0);//crc
        buffer.putInt(0);//size
        buffer.putInt(hosts.length()); //arglen
        buffer.putInt(partition.getPartitionId());
        buffer.putLong(0);//extentId
        buffer.putLong(getRequestId());
    }

    @Override
    public byte[] getBody() {
        return hosts.getBytes();
    }
}
