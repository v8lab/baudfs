package com.jd.cfs.stream;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.jd.cfs.Key;
import com.jd.cfs.dataserver.DataServer;
import com.jd.cfs.message.Protocol;
import com.jd.cfs.message.ReadRequest;
import com.jd.cfs.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ExtentReader implements InputSupplier<InputStream>, Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(ExtentReader.class);
    private Key key;
    private DataServer reader;
    private int offset, skip, length;
    private int packetReceived, packetCount;
    private boolean verifyChecksum;
    private Connection connection;
    private int socketTimeout;

    ExtentReader(DataServer dataServer, Key key, boolean verifyChecksum, int socketTimeout) {
        this.reader = dataServer;
        this.key = key;
        this.socketTimeout = socketTimeout;
        this.length = key.getSize();
        this.verifyChecksum = verifyChecksum;
        seek((int) key.getOffset());
        Preconditions.checkArgument(length >= 0);
        int requestLen = length + skip;
        packetCount = requestLen == 0 ? 0 : ((requestLen - 1) >>> Protocol.PACKET_POWER) + 1;
    }

    private void seek(int pos) {
        this.offset = pos >>> Protocol.PACKET_POWER << Protocol.PACKET_POWER;
        this.skip = pos - offset;
    }

    @Override
    public InputStream getInput() throws IOException {
        this.connection = reader.openConnection(this.socketTimeout);
        connection.connect();
        sendRequest();
        ExtentInputStream eis = this.new ExtentInputStream();
        LOG.info("Open [{}-{}] on DataServer[{}]", key.getPartitionId(), key.getExtentId(), reader);
        ByteStreams.skipFully(eis, skip);
        return ByteStreams.limit(eis, length);
    }

    private void sendRequest() throws IOException {
        ReadRequest readRequest = new ReadRequest(key, offset, length + skip);
        OutputStream os = connection.getOutputStream();
        os.write(readRequest.getHead());
        os.flush();
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.disconnect();
        }
    }

    private class ExtentInputStream extends InputStream {
        private PacketReceiver receiver;

        ExtentInputStream() throws IOException {
            receiver = new PacketReceiver(connection.getInputStream(), verifyChecksum);
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            return read(b, 0, 1) == -1 ? -1 : b[0] & 0XFF;
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            LOG.debug("ExtentReader.read:,len=" + len + ",offset==" + offset);
            int read = receiver.read(b, off, len);
            if (read > 0) {
                return read;
            }
            System.out.println(String.format("%d_%d_%d",read,packetCount,packetReceived));
            if (packetCount - packetReceived > 0) {
                receiver.nextPacket();
                packetReceived++;
                return receiver.read(b, off, len);
            }
            return -1;
        }

        @Override
        public void close() throws IOException {
            ExtentReader.this.close();
        }
    }
}