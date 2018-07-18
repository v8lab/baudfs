package com.jd.cfs.message;

import com.jd.cfs.utils.PureJavaCrc32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import static com.jd.cfs.message.Protocol.HEAD_SIZE;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public abstract class Message {

    protected final static Logger LOG       = LoggerFactory.getLogger(Message.class);
    public static AtomicLong id        = new AtomicLong(0);
    protected Checksum checksum  = new PureJavaCrc32();
    protected     long       requestId = id.getAndIncrement();
    protected     byte[]     head      = new byte[HEAD_SIZE];
    protected     byte[]     data      = null;
    protected     int        crc       = 0;

    public final long getRequestId() {
        return requestId;
    }

    public byte[] getHead() {
        return head;
    }

    public abstract byte[] getBody();
}
