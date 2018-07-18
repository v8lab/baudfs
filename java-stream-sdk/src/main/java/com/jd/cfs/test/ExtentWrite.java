package com.jd.cfs.test;

import com.jd.cfs.BlockSubmit;
import com.jd.cfs.BaudFSClient;
import com.jd.cfs.Configuration;
import com.jd.cfs.Key;
import com.jd.cfs.stream.DFSOutputStream;

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
public class ExtentWrite {
    public static BaudFSClient createClient() {
        Configuration cfg = new Configuration();
        cfg.setMaster("10.196.31.173:80;10.196.31.141:80");
        cfg.setVolName("intest");
        cfg.setZone("huitian_rack1");
        return new BaudFSClient(cfg);
    }

    public static void write(BaudFSClient client, String path) {
        final List<Key> keys = new ArrayList<>();
        DFSOutputStream cfs = client.create(new BlockSubmit() {
            @Override
            public void submit(Key key) throws IOException {
                keys.add(key);
            }
        });
        byte[] buffer = new byte[8096];
        try {
            BufferedInputStream bis = getLocalStream(path);
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

    private static BufferedInputStream getLocalStream(String path) throws Exception {
        return new BufferedInputStream(new FileInputStream(new File(path)));
    }

    public static void main(String[] args) {
        String path = args[0];
        BaudFSClient client = createClient();
        write(client, path);
    }
}
