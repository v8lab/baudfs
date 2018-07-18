package com.jd.cfs.net;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionFactory extends BasePooledObjectFactory<Connection> implements Runnable {
    private final static int UNAVAILABLE_COUNT = 10;
    private final static int MAX_CONNECT_RETRY = 32;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);
    private String ip;
    private int port;
    private int socketTimeout;
    private AtomicLong errorCount = new AtomicLong(0);
    private Thread survivalDetection;


    ConnectionFactory(String ip, int port, int socketTimeout) {
        this.ip = ip;
        this.port = port;
        this.socketTimeout = socketTimeout;
    }

    @Override
    public Connection create() throws Exception {
        checkError();
        LOG.debug("Connect to [{}]", getAddress());
        try {
            return openConnection();
        } catch (IOException e) {
            incrementError();
            throw e;
        }
    }

    private void checkError() throws IOException {
        if (getErrorCount() > UNAVAILABLE_COUNT) {
            synchronized (this) {
                if (survivalDetection == null || !survivalDetection.isAlive()) {
                    this.survivalDetection = new Thread(this, "SurvivalDetect[" + getAddress() + "]");
                    this.survivalDetection.setDaemon(true);
                    this.survivalDetection.start();
                }
            }
            throw new IOException("DataServer[" + getAddress() + "] is determined network unreachable");
        }
    }


    private Connection openConnection() throws IOException {
        Connection connection = new Connection(ip, port, socketTimeout);
        connection.connect();
        connection.setFromPool(true);
        resetError();
        return connection;
    }

    private long getErrorCount() {
        return errorCount.get();
    }

    private void incrementError() {
        errorCount.incrementAndGet();
    }

    private void resetError() {
        errorCount.set(0);
    }

    private String getAddress() {
        return ip + ":" + port;
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        p.getObject().disconnect();
        LOG.debug("Disconnect from [{}]", getAddress());
    }

    @Override
    public void run() {
        LOG.info("Attempt connect to DataServer[{}] and make it leaving an unusable state.", getAddress());
        for (int i = 0; i < MAX_CONNECT_RETRY; i++) {
            try {
                Connection connection = openConnection();
                connection.disconnect();
                LOG.info("DataServer[{}] is coming back.", getAddress());
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        }
        LOG.warn("Can't connect to DataServer[{}] after reach MAX_CONNECT_TRY={}", getAddress(), MAX_CONNECT_RETRY);
    }
}