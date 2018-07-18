package com.jd.cfs.message;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static com.jd.cfs.message.Protocol.FILE_ID_INDEX;
import static com.jd.cfs.message.Protocol.REQUEST_ID_INDEX;
import static com.jd.cfs.message.Protocol.RESULT_CODE_INDEX;

public class CreateResponse extends Message {
    private long fid;

    public CreateResponse(InputStream in) throws IOException {
        ByteStreams.readFully(in, head);
        ByteBuffer buffer = ByteBuffer.wrap(head);
        Preconditions.checkState(buffer.get() == Protocol.MAGIC, "Bad Magic");
        byte status = buffer.get(RESULT_CODE_INDEX);
        if (status != Protocol.OK) {
            long requestId = buffer.getLong(REQUEST_ID_INDEX);
            throw new IOException("Status Error=" + status + " ,Request Id = " + requestId);
        }
        this.fid = buffer.getLong(FILE_ID_INDEX);
    }


    @Override
    public byte[] getBody() {
        return new byte[0];
    }

    public long getFid() {
        return fid;
    }
}