package com.jd.cfs.dataserver;


import com.google.common.base.Preconditions;
import com.jd.cfs.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.jd.cfs.common.Constants.COLON_SEPARATOR;

public class DataServer {
    private final static Logger LOG = LoggerFactory.getLogger(DataServer.class);
    private String ip = null;
    private int port = -1;
    private String zone;

    public String getIp() {
        return this.ip;
    }

    public String getZone() {
        return zone;
    }

    public int getPort() {
        return this.port;
    }
    public Connection openConnection(int socketTimeout) throws IOException {
        return new Connection(ip, port,socketTimeout);
    }

    public DataServer(String host) {
        Preconditions.checkArgument(host != null && host.indexOf(COLON_SEPARATOR) >0,
                "illegal host:%s", host);
        String[] info = host.split(COLON_SEPARATOR);
        ip = info[0];
        if (info.length > 2){
            zone = info[2];
        }
        try {
            port = Integer.parseInt(info[1]);
        } catch (NumberFormatException e) {
            LOG.warn("create data server failed!", e.getMessage());
        }
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataServer)) return false;

        DataServer other = (DataServer) o;
        return (port == other.port) && ip.equals(other.ip);

    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + port;
        return result;
    }
}