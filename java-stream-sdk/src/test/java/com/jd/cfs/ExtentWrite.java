package com.jd.cfs;

import com.jd.cfs.stream.DFSOutputStream;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhuhongyin
 * Created on 2018/7/2.
 */
public class ExtentWrite extends BaseTest {

    @Test
    public void testWrite() {
        final List<Key> keys = new ArrayList<>();
        DFSOutputStream cfs = cfsClient.create(new BlockSubmit() {
            @Override
            public void submit(Key key) throws IOException {
                keys.add(key);
            }
        });
        byte[] buffer = new byte[8096];
        try {
            BufferedInputStream bis = getLocalStream();
            int readBytes;
            while ((readBytes = bis.read(buffer)) != -1) {
                cfs.write(buffer, 0, readBytes);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                cfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println(keys);
    }

    private BufferedInputStream getLocalStream() throws Exception {
        return new BufferedInputStream(new FileInputStream(new File("D:\\gopath.tar.gz")));
    }
}
