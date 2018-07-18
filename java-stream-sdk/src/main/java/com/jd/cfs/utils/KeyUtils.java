package com.jd.cfs.utils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.jd.cfs.Key;
import com.jd.cfs.stream.KeySeek;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.jd.cfs.common.Constants.SEMICOLON_SEPARATOR;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public class KeyUtils {
    private static final Pattern PATTERN = Pattern
            .compile("^/(jfs/b)/(\\w+)/(\\d+)/(\\d+)/(\\d+)/(\\d+)");

    public static Key newExtent(String vol, int partitionId, long fid, int size) {
        Key key = new Key();
        key.setVol(vol);
        key.setExtentId(fid);
        key.setPartitionId(partitionId);
        key.setSize(size);
        return key;
    }

    public static long getSize(List<Key> keys) {
        long size = 0;
        for (Key key : keys) {
            size += key.getSize();
        }
        return size;
    }

    public static Key parse(String key) {
        Key k = new Key();
        Matcher matcher = PATTERN.matcher(key);
        if (matcher.find()) {
            k.setVol(matcher.group(2));
            k.setPartitionId(Integer.parseInt(matcher.group(3)));
            k.setExtentId(Integer.parseInt(matcher.group(4)));
            k.setSize(Integer.parseInt(matcher.group(5)));
            k.setCrc(Integer.parseInt(matcher.group(6)));
            return k;
        }
        throw new IllegalArgumentException("Bad key :" + key);
    }

    public static List<Key> stringToKey(String key) {
        List<Key> ret = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(key)) {
            if (key.contains(SEMICOLON_SEPARATOR)) {
                String[] keyStr = key.split(SEMICOLON_SEPARATOR);
                for (String k : keyStr) {
                    ret.add(KeyUtils.parse(k));
                }
            } else {
                ret.add(KeyUtils.parse(key));
            }
        }
        return ret;
    }

    public static KeySeek seek(List<Key> keys, long fileLength, long pos) throws IOException {
        if (pos > fileLength) {
            throw new EOFException("Cannot seek after EOF");
        }

        if (pos < 0) {
            throw new EOFException("Cannot seek to negative offset");
        }

        keys = clone(keys);
        List<Key> subKeys = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            Key key = keys.get(i);
            if (key.getSize() > pos) {
                key.setOffset(pos);
                subKeys = keys.subList(i, keys.size());
                break;
            }
            pos -= key.getSize();
        }
        return new KeySeek(subKeys);
    }

    public static List<Key> slice(List<Key> keys, long start) {
        keys = clone(keys);
        List<Key> skipKeys = Lists.newArrayList();
        for (Key key : keys) {
            if (key.getSize() <= start) {
                start -= key.getSize();
                skipKeys.add(key);
                continue;
            }
            key.setOffset(start);
        }

        return keys.subList(skipKeys.size(), keys.size());
    }

    private static List<Key> clone(List<? extends Key> keys) {
        List<Key> clone = Lists.newArrayList();
        for (Key k : keys) {
            clone.add(k.clone());
        }
        return clone;
    }

    public static void main(String[] args) {
//        String key = "/777/100/100/1024/12345678";
        String key = "/jfs/b/intAst99/366/3170/10485760/0";
        Key parse = KeyUtils.parse(key);
        System.out.println(parse);
    }
}
