package com.jd.cfs.dataserver;


import com.jd.cfs.common.Constants;
import com.jd.cfs.utils.JsonConverter;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;

class VolumeLoader {
    private final static Logger LOG = LoggerFactory.getLogger(VolumeLoader.class);
    private static final String GET_DATA_PARTITIONS_PATH = "/client/dataPartitions";
    private static final String PARA_VOL_NAME = "name";
    private Master master;
    private HttpClient client = HttpClientBuilder.create().build();
    private RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(Constants.CONNECTION_TIMEOUT_MILLISECONDS)
            .setSocketTimeout(Constants.SOCKET_TIMEOUT_MILLISECONDS).build();

    VolumeLoader(String hosts) {
        this.master = new Master(hosts);
    }

    Volume load(String volName) throws IOException {
        HttpUriRequest request = RequestBuilder.get()
                .setUri(GET_DATA_PARTITIONS_PATH)
                .addParameter(PARA_VOL_NAME, volName)
                .setConfig(requestConfig)
                .build();
        byte[] reply;
        String curMasterHost = master.getLastAvailable().toHostString();
        try {
            LOG.info("loading data partitions of [{}] from {}", new Object[]{volName, curMasterHost});
            reply = getVolume(master.getLastAvailable(), request);
        } catch (IOException e) {
            LOG.warn("get data partitions err:{}", e.getMessage());
            try {
                curMasterHost = master.getLastAvailable().toHostString();
                LOG.info("loading data partitions of [{}] from {}", new Object[]{volName, curMasterHost});
                reply = getVolume(master.getLastAvailable(), request);
                LOG.info("available master change to {}", curMasterHost);
            } catch (IOException e1) {
                throw new IOException("Error get data partitions from " + curMasterHost + "Msg " + e1.getMessage());
            }

        }

        if (reply == null) {
            throw new IOException(String.format("vol name {%s} not exists", volName));
        }

        if (reply.length == 0) {
            return new Volume();
        }
        return JsonConverter.read(Volume.class, reply);
    }

    private byte[] getVolume(HttpHost curMaster, HttpUriRequest request) throws IOException {
        HttpResponse response = client.execute(curMaster, request);
        int status = response.getStatusLine().getStatusCode();
        switch (status) {
            case HttpStatus.SC_OK:
                return EntityUtils.toByteArray(response.getEntity());
            case HttpStatus.SC_FORBIDDEN:
                String newLeader = new String(EntityUtils.toByteArray(response.getEntity()), Constants.UTF_8);
                if (!"".equals(newLeader)) {
                    String newMaster = newLeader.split(Constants.LINE_BREAK)[0];
                    String[] hostWithPort = newMaster.split(Constants.COLON_SEPARATOR);
                    master.setLastAvailable(new HttpHost(hostWithPort[0], Integer.valueOf(hostWithPort[1])));
                }
                throw new ConnectException("Connection to incorrect master:" + curMaster);
            default:
                String respBody = String.format("Master {%s} reply status code {%d}  err:%s",
                        curMaster.toHostString(), status, EntityUtils.toString(response.getEntity()));
                throw new IOException(respBody);
        }
    }

}