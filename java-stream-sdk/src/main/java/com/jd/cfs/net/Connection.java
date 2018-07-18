package com.jd.cfs.net;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Connection {
    private String ip;
    private int port;
    private int socketTimeout;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private boolean isFromPool;

    public boolean isFromPool() {
        return isFromPool;
    }

    public void setFromPool(boolean fromPool) {
        isFromPool = fromPool;
    }

    public Connection(String ip, int port, int socketTimeout) {
        this.ip = ip;
        this.port = port;
        this.socketTimeout = socketTimeout;
    }

    public void connect() throws IOException {
        if (isConnected()) {
            return;
        }
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(socketTimeout);
            socket.connect(new InetSocketAddress(ip, port), socketTimeout);
            outputStream = socket.getOutputStream();
            inputStream = socket.getInputStream();
        } catch (IOException e) {
            throw new IOException("Can't connection to " + ip + ":" + port, e);
        }
    }

    public void disconnect() {
        if (isConnected()) {
            try {
                inputStream.close();
                outputStream.close();
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ignore) {
            }
        }
    }

    private boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
                && !socket.isInputShutdown() && !socket.isOutputShutdown();
    }

    public String getRemoteAddress() {
        return socket.getRemoteSocketAddress().toString();
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }
}
