package com.jd.cfs.stream;

import com.jd.cfs.BlockSubmit;
import com.jd.cfs.Key;
import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.dataserver.DataServer;
import com.jd.cfs.net.Connection;
import com.jd.cfs.utils.KeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public class DFSOutputStream extends OutputStream {
    private final static Logger LOG = LoggerFactory.getLogger(DFSOutputStream.class);
    private Set<DataPartition> excludePartitions = new HashSet<>();
    private BlockSubmit         blockSubmit;
    private ExtentWriter        currentExtentWriter;
    private int                 errorCount;
    private ExtentStoreClient esc;

    public DFSOutputStream(ExtentStoreClient esc, BlockSubmit bs) {
        this.blockSubmit = bs;
        this.esc = esc;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int total = 0, write;
        while (total < len) {
            if (currentExtentWriter == null) {
                    currentExtentWriter = allocateNewExtent();
            }
            write = currentExtentWriter.write(b, off + total, len - total);
            if (write < len - total) {
                flushCurrentExtent();
            }
            total += write;
        }
    }

    @Override
    public void close() throws IOException {
        if (currentExtentWriter != null) {
            flushCurrentExtent();
        }
    }

    private void flushCurrentExtent() throws IOException {
        IOException err = new IOException("Can't flush " + currentExtentWriter + " Reaches the maximum number of retries");
        ExtentWriter writer = currentExtentWriter;
        try {
            writer.flush();
            currentExtentWriter = null;
        } catch (IOException e) {
            err.addSuppressed(e);
            if (errorCount++ > esc.getConfiguration().getMaxRetry()) {
                throw err;
            }
            LOG.warn("Recover {} from error: {}, current retry {} time(s)", writer, e, errorCount);
            recoverExtent();
        } finally {
            writer.disconnection();
            Key key = KeyUtils.newExtent(esc.getVolName(),writer.partition.getPartitionId(), writer.fid, writer.getByteWrite());
            if (writer.getByteWrite() > 0){
                blockSubmit.submit(key);
            }
        }
    }


    private void recoverExtent() throws IOException {
        LinkedList<Packet> packets = currentExtentWriter.ackQueue;
        Packet last = currentExtentWriter.currentPacket;
        if (last != null && last.getRequestId() > packets.getLast().getRequestId()) {
            packets.addLast(last);
        }
        LOG.info("Transfer {} packets to new extent.", packets.size());
        try {
            currentExtentWriter = allocateNewExtent();
            for (Packet packet : packets) {
                write(packet.getData());
            }
        } finally {
            packets.clear();
        }
    }


    private ExtentWriter allocateNewExtent() throws IOException {
        IOException err = new IOException("Can't allocate extent after max retry.");
        for (int i = 0; i < esc.getConfiguration().getMaxRetry(); i++) {
            DataPartition partition = esc.write(excludePartitions);
            DataServer master = partition.getMaster();
            ExtentWriter extentWriter;
            Key key;
            try {
                Connection connection = master.openConnection(esc.getConfiguration().getSocketTimeout());
                connection.connect();
                long fid = setupPipeline(partition, connection);
                extentWriter = new ExtentWriter(partition, connection, fid,esc.getConfiguration().getSocketTimeout());
                LOG.info("Allocate new Extent on dataPartition={} with fid={}", partition.getPartitionId(), fid);
                key = KeyUtils.newExtent(esc.getVolName(), partition.getPartitionId(), fid, 0);
            } catch (IOException e) {
                LOG.warn("Can't allocate new extent on {} ,Current retry {} time(s)", partition, i);
                excludePartitions.add(partition);
                err.addSuppressed(e);
                continue;
            }
            blockSubmit.create(key);
            return extentWriter;
        }
        throw err;
    }

    private long setupPipeline(DataPartition partition, Connection connection) throws IOException {
        try {
            return esc.setupPipeline(partition, connection);
        } catch (Exception e) {
            connection.disconnect();
            throw new IOException("Failed setup pipeline on " + partition, e);
        }
    }
}
