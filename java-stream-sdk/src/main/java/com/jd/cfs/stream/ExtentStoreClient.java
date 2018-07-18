package com.jd.cfs.stream;

import com.google.common.base.Preconditions;
import com.jd.cfs.Configuration;
import com.jd.cfs.Key;
import com.jd.cfs.Selector;
import com.jd.cfs.dataserver.DataPartition;
import com.jd.cfs.dataserver.DataServer;
import com.jd.cfs.message.DeleteRequest;
import com.jd.cfs.message.CreateRequest;
import com.jd.cfs.message.CreateResponse;
import com.jd.cfs.net.Connection;
import com.jd.cfs.selector.StandardSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public class ExtentStoreClient {

    private final static Logger LOG = LoggerFactory.getLogger(ExtentStoreClient.class);

    private Selector selector;
    private Configuration cfg;

    public ExtentStoreClient(Configuration config) {
        this.cfg = config;
        this.selector = new StandardSelector(config.getMaster());
    }

     Configuration getConfiguration() {
        return this.cfg;
    }

     DataPartition write(Set<DataPartition> exclude) throws IOException {
        return selector.selectForWrite(this.cfg.getVolName(), exclude);
    }

    public DataPartition select(int volId) throws IOException {
        return selector.selectForRead(this.cfg.getVolName(), volId);
    }

    public void delete(Key key) throws IOException {
        String volName = this.cfg.getVolName();
        Preconditions.checkArgument(key.getVol().equals(volName), "Invalid key for:" + volName);
        DataPartition partition = selector.selectForRead(volName, key.getPartitionId());
        if (partition != null) {
            DeleteRequest request = new DeleteRequest(key);
            for (DataServer dataServer : partition.getServers()) {
                try {
                    Connection connection = dataServer.openConnection(this.cfg.getSocketTimeout());
                    connection.connect();
                    connection.getOutputStream().write(request.getHead());
                    connection.getOutputStream().flush();
                    connection.disconnect();
                } catch (IOException ignore) {
                    LOG.error("delete key{} from dataServer {} failed", key, dataServer.getIp());
                    throw ignore;
                }
            }
        }
    }

    public ExtentInputStream read(Key key) throws IOException {
        Preconditions.checkArgument(key.getVol().equals(getVolName()), "Invalid key for:" + getVolName());
        DataPartition partition = selector.selectForRead(getVolName(), key.getPartitionId());
        if (partition == null) {
            throw new IOException("No Such dataPartition = " + key.getPartitionId());
        }
        return new ExtentInputStream(partition, key, cfg.isVerifyChecksum());
    }

     long setupPipeline(DataPartition partition, Connection connection) throws IOException {
        LOG.info("Setup pipeline to {}", partition);
        CreateRequest request = new CreateRequest(partition);
        OutputStream os = connection.getOutputStream();
        os.write(request.getHead());
        os.write(request.getBody());
        os.flush();
        CreateResponse response = new CreateResponse(connection.getInputStream());
        return response.getFid();
    }

     String getVolName() {
        return this.cfg.getVolName();
    }

}
