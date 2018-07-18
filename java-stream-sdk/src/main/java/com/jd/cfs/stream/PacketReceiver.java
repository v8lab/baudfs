package com.jd.cfs.stream;

import com.google.common.io.ByteStreams;
import com.jd.cfs.message.Protocol;
import com.jd.cfs.utils.PureJavaCrc32;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

class PacketReceiver {
    private int length;
    private int crc;
    private boolean  verifyChecksum = true;
    private Checksum checksum       = new PureJavaCrc32();
    private byte[]   head           = new byte[Protocol.HEAD_SIZE];
    private byte[]   body           = new byte[Protocol.EXTENT_PACKET_SIZE];
    private ByteBuffer buffer;
    private InputStream input;

    public PacketReceiver(InputStream in, boolean verifyChecksum) {
        input = in;
        this.verifyChecksum = verifyChecksum;
    }

    int read(byte[] b, int offset, int len) {
        if (buffer == null) {
            return -1;
        }
        len = Math.min(buffer.remaining(), len);
        buffer.get(b, offset, len);
        return len;
    }

    int nextPacket() throws IOException {
        ByteStreams.readFully(input, head);
        resolve();
        ByteStreams.readFully(input, body, 0, length);
        if (verifyChecksum) {
            checksum.reset();
            checksum.update(body, 0, length);
            if (crc != (int) checksum.getValue()) {
                throw new IOException("Bad Crc");
            }
        }
        buffer = ByteBuffer.wrap(body, 0, length);
        return length;
    }

    private void resolve() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(head);
        if (buffer.get(Protocol.MAGIC_INDEX) != Protocol.MAGIC){
            throw new IOException("Bad Magic");
        }
        if (buffer.get(Protocol.RESULT_CODE_INDEX) != Protocol.OK) {
            long requestId = buffer.getLong(Protocol.REQUEST_ID_INDEX);
            throw new IOException("Status Code = " + buffer.get(Protocol.RESULT_CODE_INDEX) + " ,RID=" + requestId);
        }
        crc = buffer.getInt(Protocol.CRC_INDEX);
        length = buffer.getInt(Protocol.SIZE_INDEX);
        if (length > Protocol.EXTENT_PACKET_SIZE) {
            throw new IOException("Packet Length=" + length + " > " + Protocol.EXTENT_PACKET_SIZE);
        }
    }
    
     void close() throws IOException {
    	 if(input!=null){
    		 input.close();
    	 }
     }
}