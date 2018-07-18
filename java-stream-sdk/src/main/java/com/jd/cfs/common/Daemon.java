package com.jd.cfs.common;


public class Daemon extends Thread {
    public Daemon(){
        setDaemon(true);
    }
}