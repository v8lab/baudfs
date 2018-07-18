package com.jd.cfs.message;


import com.jd.cfs.Key;

import java.nio.ByteBuffer;

public class ReadRequest extends Message {

    public ReadRequest(Key key, long offset, int size) {
        ByteBuffer buffer = ByteBuffer.wrap(head);
        buffer.put(Protocol.MAGIC);//magic
        buffer.put(Protocol.EXTENT);//store mode
        buffer.put(Protocol.Command.SRead.code);//opcode
        buffer.put((byte) 0); // result code
        buffer.put((byte) 0); // nodes
        buffer.putInt(0);//crc
        buffer.putInt(size);
        buffer.putInt(0);//arglen
        buffer.putInt(key.getPartitionId());
        buffer.putLong(key.getExtentId());
        buffer.putLong(offset);
        buffer.putLong(getRequestId());
    }

    @Override
    public byte[] getBody() {
        return new byte[0];
    }
}
