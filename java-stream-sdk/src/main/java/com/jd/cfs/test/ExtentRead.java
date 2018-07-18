package com.jd.cfs.test;

import com.jd.cfs.BaudFSClient;
import com.jd.cfs.stream.DFSInputStream;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by zhuhongyin
 * Created on 2018/7/5.
 */
public class ExtentRead {
    public static void read(BaudFSClient client, String key, String path) {
        DFSInputStream inputStream = client.open(key);
        try {
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedOutputStream bw = new BufferedOutputStream(
                    new FileOutputStream(file));
            byte[] buf = new byte[4096];
            int readLen = 0;

            while ((readLen = inputStream.read(buf, 0, buf.length)) > 0) {
                bw.write(buf,0,readLen);
            }
            bw.flush();
            bw.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length != 2){
            System.out.println("must have key and path parameter");
            System.exit(0);
        }
        String key = args[0];
        String path = args[1];
        BaudFSClient client = ExtentWrite.createClient();
        read(client,key,path);
    }
}
