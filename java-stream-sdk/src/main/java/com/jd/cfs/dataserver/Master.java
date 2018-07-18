package com.jd.cfs.dataserver;


import com.google.common.collect.Lists;
import com.jd.cfs.common.Constants;
import org.apache.http.HttpHost;

import java.util.List;

public class Master {
    private List<HttpHost> hosts = Lists.newArrayList();
    private HttpHost lastAvailable = null;

     HttpHost getLastAvailable() {
        return this.lastAvailable;
    }

     void setLastAvailable(HttpHost lastAvailable) {
        this.lastAvailable = lastAvailable;
    }

     Master(String masters) {
        for (String host : masters.split(Constants.SEMICOLON_SEPARATOR)) {
            String realHost = host;
            int port = 80;
            if (host.contains(Constants.COLON_SEPARATOR)) {
                String[] hostWithPort = host.split(Constants.COLON_SEPARATOR);
                realHost = hostWithPort[0];
                port = Integer.valueOf(hostWithPort[1]);
            }
            hosts.add(new HttpHost(realHost, port));
        }
        lastAvailable = hosts.get(0);
    }

    @Override
    public String toString() {
        return "Master{" +
                "hosts=" + hosts +
                '}';
    }
}
