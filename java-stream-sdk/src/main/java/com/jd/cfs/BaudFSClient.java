package com.jd.cfs;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.jd.cfs.stream.DFSInputStream;
import com.jd.cfs.stream.DFSOutputStream;
import com.jd.cfs.stream.ExtentStoreClient;
import com.jd.cfs.utils.KeyUtils;

import java.io.IOException;
import java.util.List;

import static com.jd.cfs.common.Constants.UNDERLINE_SEPARATOR;

/**
 * Created by zhuhongyin
 * Created on 2018/6/28.
 */
public class BaudFSClient {
    private ExtentStoreClient esc;

    public BaudFSClient(Configuration config) {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(config.getVolName()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(config.getMaster()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(config.getZone())
                && config.getZone().split(UNDERLINE_SEPARATOR).length == 2);
        this.esc = new ExtentStoreClient(config);
    }

    public DFSOutputStream create(BlockSubmit bs) {
        return new DFSOutputStream(this.esc, bs);
    }

    public DFSInputStream open(String key) {
        List<Key> keys = KeyUtils.stringToKey(key);
        return open(keys);
    }

    public DFSInputStream open(Key key) {
        return open(Lists.newArrayList(key));
    }

    public DFSInputStream open(List<Key> keys) {
        return new DFSInputStream(esc, keys);
    }

    public void delete(String key) throws IOException {
        delete(KeyUtils.stringToKey(key));
    }

    public void delete(Key key) throws IOException {
        delete(Lists.newArrayList(key));
    }

    public void delete(List<Key> keys) throws IOException {
        for (Key key : keys) {
            esc.delete(key);
        }
    }

}
