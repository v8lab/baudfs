package com.jd.cfs.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.jd.cfs.Key;
import com.jd.cfs.utils.KeyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public class DFSInputStream extends InputStream implements Seekable {

    private ExtentStoreClient esc;
    private List<Key> keys;
    private long length;
    private long position;
    private InputStream input;

    public DFSInputStream(ExtentStoreClient esc, List<Key> keys) {
        Preconditions.checkNotNull(keys, "Keys should not be NULL.");
        this.esc = esc;
        this.keys = keys;
        this.length = KeyUtils.getSize(keys);
    }

    private InputStream openStream(List<Key> keys) throws IOException {
        List<InputSupplier<InputStream>> inputs = Lists.newArrayList();
        for (final Key key : keys) {
            inputs.add(esc.read(key));
        }
        return ByteStreams.join(inputs).getInput();
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int i = read(b, 0, 1);
        if (i == -1) {
            return i;
        }
        return b[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (input == null) {
            input = openStream(keys);
        }
        len = input.read(b, off, len);
        if (len != -1) {
            position += len;
        }
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        Preconditions.checkArgument(n >= 0, "skip is a negative number");
        if (n == 0) {
            return 0;
        }
        long pos = position;
        long seekTo = Math.min(position + n, length);
        KeySeek seek = KeyUtils.seek(keys, length, seekTo);
        if (input == null) {
            input = openStream(seek.keys);
        } else {
            ByteStreams.skipFully(input, seekTo - pos);
        }
        position = seekTo;
        return seekTo - pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        KeySeek seek = KeyUtils.seek(keys, length, pos);
        if (input != null) {
            input.close();
        }
        input = openStream(seek.keys);
        position = pos;
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        seek(targetPos);
        return true;
    }
}
