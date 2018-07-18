package com.jd.cfs.message;


import com.jd.cfs.Key;

import java.nio.ByteBuffer;

public class DeleteRequest extends Message {

    public DeleteRequest(Key key) {
        ByteBuffer buffer = ByteBuffer.wrap(head);
        buffer.put(Protocol.MAGIC); //magic
        buffer.put(Protocol.EXTENT); //store mode
        buffer.put(Protocol.Command.Delete.code); //opcode
        buffer.put((byte) 0); // result code
        buffer.put((byte) 0); // nodes
        buffer.putInt(0); //crc
        buffer.putInt(0); //size
        buffer.putInt(0); //arglen
        buffer.putInt(key.getPartitionId());
        buffer.putLong(key.getExtentId());
        buffer.putLong(0); //offset
        buffer.putLong(getRequestId()); //requestId
    }

    @Override
    public byte[] getBody() {
        return new byte[0];
    }
}
