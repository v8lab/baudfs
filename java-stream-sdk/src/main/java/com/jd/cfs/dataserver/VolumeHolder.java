package com.jd.cfs.dataserver;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.jd.cfs.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VolumeHolder {
    private final static Logger LOG = LoggerFactory.getLogger(VolumeHolder.class);
    private VolumeLoader loader;
    private Map<String, Volume> vols = Maps.newHashMap();
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public VolumeHolder(String master) {
        this.loader = new VolumeLoader(master);
    }

    private LoadingCache<String, Volume> cache = CacheBuilder.newBuilder()
            .refreshAfterWrite(Constants.VOLUME_CACHE_REFRESH_INTERVAL, TimeUnit.MINUTES)
            .expireAfterWrite(Constants.VOLUME_CACHE_EXPIRED_INTERVAL, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Volume>() {
                @Override
                public Volume load(String cluster) throws Exception {
                    return loadVolume(cluster);
                }

                @Override
                public ListenableFuture<Volume> reload(final String clusterId, Volume oldValue) throws Exception {
                    LOG.debug("reload...");
                    // asynchronous!
                    ListenableFutureTask<Volume> task = ListenableFutureTask.create(new Callable<Volume>() {
                        @Override
                        public Volume call() {
                            Volume volume;
                            try {
                                volume = loadVolume(clusterId);
                            } catch (Exception e) {
                                volume = null;
                                LOG.error("reload volume error.", e);
                            }
                            return volume;
                        }
                    });
                    executor.execute(task);
                    return task;
                }
            });

    public Volume getVolume(String volName) throws IOException {
        try {
            return cache.get(volName);
        } catch (Exception e) {
            throw new IOException("can't get volume by: " + volName, e.getCause());
        }
    }

    private Volume loadVolume(String volName) throws Exception {
        Volume newVolume;
        try {
            newVolume = loader.load(volName);
            Volume oldVolume = vols.get(volName);
            if (oldVolume != null) {
                newVolume.resetDataPartitions(oldVolume);
            }
            newVolume.sort();
            vols.put(volName, newVolume);
            LOG.debug("load volume with {},Total {} dataPartitions, {} writable dataPartitions.",
                    volName, newVolume.getDataPartitions().size(), newVolume.getWritable().size());
            return newVolume;
        } catch (Exception e) {
            LOG.warn("fail load volume{},exception{}", volName, e);
            newVolume = vols.get(volName);
            if (newVolume == null) throw e;
            return newVolume;
        }
    }


}
