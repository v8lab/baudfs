package com.jd.cfs.stream;

import com.google.common.base.Preconditions;
import com.jd.cfs.common.Daemon;
import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.message.Protocol;
import com.jd.cfs.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ExtentWriter {
    private final static Logger LOG                   = LoggerFactory.getLogger(ExtentWriter.class);
    private final static int    MAX_PACKET_PER_EXTENT = 1024;
    private final static int    MAX_UNACK_PACKET      = 80;
    private final static int    MAX_RECOVER_COUNT     = 3;
    DataPartition partition;
    Packet currentPacket;
    long    fid;
    private int offset, byteAck, serialNumber, packetSend, packetAck, recover;
    final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
    private DataStreamer dataStreamer;
    private final ReentrantLock lock     = new ReentrantLock(false);
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull  = lock.newCondition();
    private int socketTimeout;

    public ExtentWriter(DataPartition partition, Connection connection, long fid,int socketTimeout) {
        this.partition = partition;
        this.fid = fid;
        this.socketTimeout = socketTimeout;
        dataStreamer = new DataStreamer(connection);
        dataStreamer.start();
    }

    int write(byte[] data, int offset, int len) {
        int total = 0, write;
        while (total < len && !isFull()) {
            if (currentPacket == null) {
                currentPacket = makeNewPacket();
            }
            write = currentPacket.fill(data, offset + total, len - total);
            if (write < len - total) {
                Packet packet = currentPacket;
                try {
                    sendCurrentPacket();
                } catch (IOException e) {
                    LOG.warn("Send {} to {} Error:{} try recover.", packet, toString(), e.getMessage());
                    dataStreamer.close();
                    dataStreamer.interrupt();
                    if (!recover()) break;
                }
            }
            total += write;
        }
        return total;
    }

    boolean recover() {
        while (recover++ < MAX_RECOVER_COUNT) {
            LOG.info("Try recover current {}, packet resend={}, packet ack={}, retry {} time(s)",
                    toString(), ackQueue.size(), packetAck, recover);
            try {
                Connection connection = partition.getMaster().openConnection(this.socketTimeout);
                connection.connect();
                dataStreamer = new DataStreamer(connection);
                dataStreamer.start();
                for (Object item : ackQueue.toArray()) {
                    Packet packet = (Packet) item;
                    dataStreamer.sendPacket(packet);
                    LOG.info("Resend {}", packet);
                }
                return true;
            } catch (Exception e) {
                LOG.warn("Failed recover {} packet resend={}, packet ack={},Error:{} ",
                        toString(), ackQueue.size(), packetAck, e.getMessage());
                dataStreamer.close();
                dataStreamer.interrupt();
            }
        }
        return false;
    }


    private Packet makeNewPacket() {
        return new Packet(partition, fid, this.serialNumber++, offset);
    }

    private void sendCurrentPacket() throws IOException {
        if (currentPacket != null) {
            try {
                queuePacket(currentPacket);
                packetSend++;
                Packet packet = currentPacket;
                currentPacket = null;
                offset += packet.getDataLen();
                dataStreamer.sendPacket(packet);
                LOG.debug("Send {}", packet);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    private void queuePacket(Packet packet) throws InterruptedException, IOException {
        this.lock.lockInterruptibly();
        try {
            while (ackQueue.size() == MAX_UNACK_PACKET) {
                if (!notFull.await(5, TimeUnit.SECONDS)) {
                    if (dataStreamer.isClose()) {
                        throw new IOException("Stream is closed.");
                    }
                }
            }
            ackQueue.addLast(packet);
            notEmpty.signal();
        } finally {
            this.lock.unlock();
        }
    }

    private Packet dequeueFirstPacket() throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            while (ackQueue.size() == 0) {
                notEmpty.await();
            }
            return ackQueue.getFirst();
        } finally {
            this.lock.unlock();
        }
    }

    private void ackFirstPacket() throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            Packet packet = ackQueue.removeFirst();
            byteAck += packet.getDataLen();
            packetAck++;
            notFull.signal();
            LOG.debug("Ack packet={},unack={}", packet.serialnumber, ackQueue.size());
        } finally {
            this.lock.unlock();
        }
    }

    private boolean isFull() {
        return packetSend == MAX_PACKET_PER_EXTENT;
    }

    public void flush() throws IOException {
        LOG.info("Flush {},current={},packet send={},packet ack={} ", this.toString(), currentPacket, packetSend, packetAck);
        try {
            sendCurrentPacket();
            dataStreamer.close();
            dataStreamer.extentStream.flush();
            if (packetSend == packetAck) {
                dataStreamer.interrupt();
                return;
            }
            dataStreamer.join();
            if (!allFlushed()) {
                recover();
                dataStreamer.close();
                dataStreamer.join();
                if (!allFlushed()) {
                    throw new IOException("Send:" + packetSend + ",Ack:" + packetAck + ",Remain:" + ackQueue.size() + ",CurrentPacket:" + currentPacket);
                }
            }
        } catch (IOException e) {
            if (!recover()) throw e;
            flush();
        } catch (InterruptedException e) {
            throw new IOException(e);//write thread interrupted should never happen
        }
    }

    private boolean allFlushed() {
        return !(packetSend != packetAck || ackQueue.size() > 0 || currentPacket != null);
    }

    void disconnection() {
        dataStreamer.disconnection();
    }

    public int getByteWrite() {
        return byteAck;
    }


    @Override
    public String toString() {
        return "Extent{DataPartition=" + partition.getPartitionId() + ",FID=" + fid + "}";
    }


    private class DataStreamer extends Daemon {
        private OutputStream extentStream;
        private DataInputStream extentReplyStream;
        private Connection      connection;
        private volatile boolean close = false;
        private          byte[]  head  = new byte[Protocol.HEAD_SIZE];

        public DataStreamer(Connection connection) {
            super();
            this.connection = connection;
            extentStream = connection.getOutputStream();
            extentReplyStream = new DataInputStream(new BufferedInputStream(connection.getInputStream()));
            setName("Streamer:" + partition.getPartitionId() + "-" + fid);
        }

        boolean isClose(){
            return close == true;
        }

        void sendPacket(Packet packet) throws IOException {
            if (isClose()) {
                throw new IOException("Streamer is close.");
            }
            packet.writeTo(extentStream);
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted() && (!isClose() || ackQueue.size() > 0)) {
                try {
                    Packet packet = dequeueFirstPacket();
                    extentReplyStream.readFully(head);
                    processReply(head, packet);
                    ackFirstPacket();
                } catch (Exception e) {
                    e.printStackTrace();
                    if (ackQueue.size() > 0) {
                        LOG.warn("Streamer is unexpected close. Unack packet={},Error:{}",
                                ackQueue.size(), e.getMessage() == null ? e.getClass().getName() : e.getMessage());
                    }
                    break;
                }
            }
            close();
        }

        private void processReply(byte[] head, Packet packet) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(head);
            long requestId = buffer.getLong(Protocol.REQUEST_ID_INDEX);
            if (buffer.get(Protocol.MAGIC_INDEX) != Protocol.MAGIC) throw new IOException("Bad Magic");
            if (buffer.get(Protocol.RESULT_CODE_INDEX) != Protocol.OK) {
                throw new IOException("Status Code = " + buffer.get(Protocol.RESULT_CODE_INDEX) + " RequestId=" + requestId);
            }
            Preconditions.checkState(requestId == packet.getRequestId(),
                    "RequestId Mismatch source[%s],reply[%s]", packet.getRequestId(), requestId);
        }
        void disconnection() {
            this.connection.disconnect();
        }
        void close() {
            this.close = true;
        }
    }
}