package com.jd.cfs.stream;

import com.google.common.io.InputSupplier;
import com.jd.cfs.Key;
import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.dataserver.DataServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhuhongyin
 * Created on 2018/7/2.
 */
public class ExtentInputStream extends InputStream implements InputSupplier<InputStream> {

    private final static Logger LOG = LoggerFactory.getLogger(ExtentInputStream.class);
    private Key key;
    private InputStream input;
    private boolean verifyChecksum;
    private DataPartition partition;
    private Set<DataServer> exclude = new HashSet<>();
    private DataServer currentDataServer;

    ExtentInputStream(DataPartition partition, Key key, boolean verifyChecksum) {
        this.partition = partition;
        this.key = key.clone();
        this.verifyChecksum = verifyChecksum;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int len = read(b, 0, 1);
        if (len > 0) {
            return b[0] & 0xFF;
        }
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkStream();
        try {
            return input.read(b, off, len);
        } catch (IOException e) {
            addExclude(currentDataServer);
            LOG.warn("", e);
            input.close();
            getInput();
            return read(b, off, len);
        }
    }

    @Override
    public InputStream getInput() throws IOException {
        IOException err = new IOException("Can't open stream from " + partition + " with " + key);
        while ((currentDataServer = this.partition.read("", exclude)) != null) {
            ExtentReader reader = new ExtentReader(currentDataServer, key, verifyChecksum, 0);
            try {
                input = reader.getInput();
                return this;
            } catch (IOException e) {
                LOG.warn("Fail read from DataServer[{}],Error:{}", currentDataServer, e.getMessage());
                err.addSuppressed(e);
                reader.close();
                addExclude(currentDataServer);
            }
        }
        throw err;
    }

    @Override
    public void close() throws IOException {
        if (input != null) {
            input.close();
        }
    }

    private void addExclude(DataServer dataServer) {
        this.exclude.add(dataServer);
    }

    private void checkStream() throws IOException {
        if (input == null) {
            throw new IOException("Not open stream");
        }
    }
}
