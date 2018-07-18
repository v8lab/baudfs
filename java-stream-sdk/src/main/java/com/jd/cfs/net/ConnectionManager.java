package com.jd.cfs.net;

import com.jd.cfs.Configuration;
import com.jd.cfs.dataserver.DataServer;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {
    private ConcurrentHashMap<DataServer, GenericObjectPool<Connection>> dataSources = new ConcurrentHashMap<>();
    private Configuration conf;

    public ConnectionManager(Configuration configuration) {
        this.conf = configuration;
    }

    public Connection getConnection(DataServer dataServer) throws IOException {
        GenericObjectPool<Connection> pool = dataSources.get(dataServer);
        if (pool == null) {
            synchronized (this) {
                pool = dataSources.get(dataServer);
                if (pool == null) {
                    pool = makePool(dataServer);
                    GenericObjectPool<Connection> oldPool = dataSources.putIfAbsent(dataServer, pool);
                    if (oldPool != null) {
                        pool = oldPool;
                    }
                }
            }
        }
        Connection connection;
        try {
            connection = pool.borrowObject();
        } catch (Exception e) {
            throw new IOException("borrow connection failed", e);
        }

        return connection;
    }

    public void returnConnection(DataServer dataServer, Connection connection) {
        GenericObjectPool<Connection> pool = dataSources.get(dataServer);
        if (pool != null) {
            pool.returnObject(connection);
            return;
        }
        connection.disconnect();
    }

    public void destroyConnection(DataServer dataServer, Connection connection) {
        GenericObjectPool<Connection> pool = dataSources.get(dataServer);

        if (pool == null) {
            connection.disconnect();
            return;
        }
        try {
            pool.invalidateObject(connection);
        } catch (Exception e) {
            connection.disconnect();
        }
    }

    public void close(DataServer dataServer) {
        GenericObjectPool<Connection> pool = dataSources.remove(dataServer);
        if (pool != null) {
            pool.close();
        }
    }

    private GenericObjectPool<Connection> makePool(DataServer dataServer) {
        return new GenericObjectPool<>(new ConnectionFactory(dataServer.getIp(), dataServer.getPort(), conf.getSocketTimeout()), conf.getPoolConf());
    }
}