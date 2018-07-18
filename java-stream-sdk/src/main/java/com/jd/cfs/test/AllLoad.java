package com.jd.cfs.test;

import com.jd.cfs.BaudFSClient;
import com.jd.cfs.BlockSubmit;
import com.jd.cfs.Configuration;
import com.jd.cfs.Key;
import com.jd.cfs.stream.DFSInputStream;
import com.jd.cfs.stream.DFSOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhuhongyin
 * Created on 2018/7/13.
 */
public class AllLoad {

    static Map<Long, List<Key>> readKeys = new ConcurrentHashMap<>();
    static Map<Long, List<Key>> deleteKeys = new ConcurrentHashMap<>();
    static AtomicLong lastReadKey = new AtomicLong();
    static AtomicLong lastDeleteKey = new AtomicLong();
    static final int ONE_MB = 1024 * 1024;
    static final int MAX_BUF_SIZE = 5 * 1024 * 1024;
    static final int PAUSE_INTERNAL = 5000;
    static final int DELETE_PERCENT = 3;
    static boolean recordNoSuchFile = false;
    static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) {
        String master = args[0];
        int writeThreadNum = Integer.parseInt(args[1]);
        int readTheadNum = Integer.parseInt(args[2]);
        int deleteThreadNum = Integer.parseInt(args[3]);
        recordNoSuchFile = Boolean.parseBoolean(args[4]);
        System.out.println("master address:" + master);
        System.out.println("write thread num:" + writeThreadNum);
        System.out.println("read thread num:" + readTheadNum);
        System.out.println("delete thread num:" + deleteThreadNum);
        System.out.println("recordNoSuchFile:" + recordNoSuchFile);
        int threadNum = writeThreadNum + readTheadNum + deleteThreadNum;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        try {
            allLoad(master, writeThreadNum, readTheadNum, deleteThreadNum, executorService);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void allLoad(String master, int writeThreadNum, int readThreadNum, int deleteThreadNum,
                                ExecutorService executorService) throws Exception {
        BaudFSClient jfsClient = createJfsClient(master);
        for (int i = 0; i < writeThreadNum; i++) {
            executorService.submit(new ConcurrentWrite(jfsClient));
        }
        for (int i = 0; i < readThreadNum; i++) {
            executorService.submit(new ConcurrentRead(jfsClient));
        }

        for (int i = 0; i < deleteThreadNum; i++) {
            executorService.submit(new ConcurrentDelete(jfsClient));
        }

    }

    private static BaudFSClient createJfsClient(String master) {
        Configuration config = new Configuration();
        config.setMaster("10.196.31.173:80;10.196.31.141:80");
        config.setZone("beijing_majuqiao1");
        config.setVolName("intest");
        return new BaudFSClient(config);
    }

    private static List<Key> getKeyForReadFromCache() {
        if (readKeys.size() == 0) {
            return null;
        }
        List<Key> keys;
        if (readKeys.size() > 100) {
            keys = readKeys.remove(lastReadKey.getAndIncrement());
        } else {
            keys = readKeys.get(lastReadKey.get());
        }
        return keys;
    }

    private static List<Key> getKeysForDeleteFromCache() {
        synchronized (AllLoad.class) {
            if (deleteKeys.size() == 0) {
                return null;
            }
            return deleteKeys.remove(lastDeleteKey.getAndIncrement());
        }
    }

    static class ConcurrentDelete implements Callable<String> {
        private BaudFSClient jfsClient;

         ConcurrentDelete(BaudFSClient jfsClient) {
            this.jfsClient = jfsClient;
        }

        @Override
        public String call() throws Exception {
            for (int i = 0; ; i++) {
                try {
                    if (i == Integer.MAX_VALUE) {
                        i = 0;
                    }
                    List<Key> keys = getKeysForDeleteFromCache();
                    if (keys == null) {
                        Thread.sleep(2000L);
                        continue;
                    }
//                    System.out.println("delete from keys:" + keys);
                    jfsClient.delete(keys);
                    if (i % DELETE_PERCENT == 0) {
                        read(jfsClient, keys);
                    }
                } catch (Exception e) {
                    System.out.println(formatCurrentTime() + " AllLoadDelete failed:" + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }


    private static void read(BaudFSClient jfsClient, List<Key> keys) throws Exception {
        try {
            DFSInputStream dfsInputStream = jfsClient.open(keys);
            dfsInputStream.read();
        } catch (IOException e) {
            boolean recordToLog = true;
            for (Throwable suppressed : e.getSuppressed()) {
                if (suppressed.getCause() != null && !(suppressed.getCause() instanceof FileNotFoundException)) {
                    recordToLog = true;
                    break;
                }
                recordToLog = false;
            }

            if (recordToLog || recordNoSuchFile) {

                System.out.println(formatCurrentTime() + " AllLoadRead failed,key:" + keys);
                throw e;
            }
        }
    }

    static class ConcurrentRead implements Callable<String> {
        private BaudFSClient jfsClient;

         ConcurrentRead(BaudFSClient jfsClient) {
            this.jfsClient = jfsClient;
        }

        @Override
        public String call() throws Exception {
            for (int i = 0; ; i++) {
                try {
                    if (i == Integer.MAX_VALUE) {
                        i = 0;
                    }
                    List<Key> keys = getKeyForReadFromCache();
                    if (keys == null) {
                        Thread.sleep(10L);
                        continue;
                    }
                    if (i % PAUSE_INTERNAL == 0) {
                        Thread.sleep(100L);
                    }
//                    System.out.println("read from keys:" + keys);
                    read(jfsClient, keys);
                } catch (Exception e) {
                    System.out.println(formatCurrentTime() + " read occurred other exception:" + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }


    static class ConcurrentWrite implements Callable<List<Key>> {
        private BaudFSClient jfsClient;


         ConcurrentWrite(BaudFSClient jfsClient) {
            this.jfsClient = jfsClient;
        }


        @Override
        public List<Key> call() throws Exception {
            for (long i = 0; ; i++) {
                try {
                    if (i == Integer.MAX_VALUE) {
                        i = 0;
                    }
                    if (i % PAUSE_INTERNAL == 0) {
                        Thread.sleep(10L);
                    }
                    long seed = System.currentTimeMillis();
                    List<Key> keyList = write(jfsClient, seed);
//                    System.out.println("write keys:" + keyList);
                    if (keyList == null || keyList.size() == 0) {
                        continue;
                    }

                    if (i % DELETE_PERCENT == 0) {
                        deleteKeys.put(i, keyList);
                    } else {
                        readKeys.put(i, keyList);
                    }

                } catch (Exception e) {
                    System.out.println(formatCurrentTime() + " write occurred other exception:" + e.getMessage());
                    e.printStackTrace();
                }
            }

        }
    }

    private static List<Key> write(BaudFSClient jfsClient, long randomSeed) throws Exception {
        try {
            final List<Key> keyList = new ArrayList<>();
            Random random = new Random(randomSeed);
            double bufSizeRandom = Math.random();
            int length = 0;
            if (bufSizeRandom >= 0.9) {
                length = MAX_BUF_SIZE;
            } else {
                length = (int) ((bufSizeRandom * 300 + 1) * ONE_MB);
            }
            byte[] buf = new byte[length];
            for (int i = 0; i < buf.length; i++) {
                buf[i] = (byte) random.nextInt();
            }
            DFSOutputStream dfsOutputStream = jfsClient.create(new BlockSubmit() {
                @Override
                public void submit(Key key) throws IOException {
                    keyList.add(key);
                }
            });
            dfsOutputStream.write(buf);
            return keyList;
        } catch (IOException e) {
            System.out.println(formatCurrentTime() + " AllLoadWrite failed" + e.getMessage());
            throw e;
        }

    }

     static String formatCurrentTime() {
        SimpleDateFormat time = new SimpleDateFormat(DATETIME_FORMAT);
        return time.format(System.currentTimeMillis());
    }
}
