package com.jd.cfs.test;

import com.jd.cfs.BaudFSClient;

import java.io.IOException;

/**
 * Created by zhuhongyin
 * Created on 2018/7/5.
 */
public class ExtentDelete {
    private static void delete(BaudFSClient client, String key) {
        try {
            client.delete(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        if (args == null || args.length != 1){
            System.out.println("must have key parameter");
            System.exit(0);
        }
        String key = args[0];
        BaudFSClient client = ExtentWrite.createClient();
        delete(client,key);
    }
}
