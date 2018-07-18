package com.jd.cfs;

import java.io.IOException;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public abstract class BlockSubmit {
    public abstract void submit(Key key) throws IOException;

    public void create(Key key) throws IOException {
    }
}
