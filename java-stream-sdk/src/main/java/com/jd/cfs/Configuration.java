package com.jd.cfs;

import com.google.common.base.Preconditions;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by zhuhongyin
 * Created on 2018/6/29.
 */
public class Configuration {
    private int maxRetry = 32;
    private boolean blockWhenExhausted = false;
    private int maxActive = 100;               //最大活动连接数,负数不限制
    private int maxWait = 2000;              //当whenExhaustedAction=1时候阻塞最长等待时间，负数用不超时
    private int maxIdle = 80;                //最大空闲连接，负数没有上限
    private int minIdle = 0;                 //最小空闲连接
    private int timeBetweenEvictionRunsMillis = 10000;             //后台每隔多少毫秒进行连接池清理
    private int numTestsPerEvictionRun = 5;                 //每次清理的个数
    private int minEvictableIdleTimeMillis = 1000;              //如果清理的时候连接空闲时间大于这个值，连接将会被关闭
    private int socketTimeout = 50000;              //Socket Timeout

    private boolean testOnBorrow = true;
    private boolean testOnCreate = false;
    private boolean testOnReturn = false;

    private String master;
    private String volName;
    private String zone;

    private boolean verifyChecksum     = true;

    public boolean isVerifyChecksum() {
        return verifyChecksum;
    }

    public void setVerifyChecksum(boolean verifyChecksum) {
        this.verifyChecksum = verifyChecksum;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestOnCreate() {
        return testOnCreate;
    }

    public void setTestOnCreate(boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public int getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public int getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setVolName(String volName) {
        this.volName = volName;
    }

    public String getVolName() {
        return volName;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public int getMaxRetry() {
        return maxRetry;
    }


    public void setMaster(String master) {
        this.master = master;
    }

    public String getMaster() {
        return master;
    }


    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public GenericObjectPoolConfig getPoolConf() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnCreate(testOnCreate);
        config.setTestOnReturn(testOnReturn);
        config.setBlockWhenExhausted(blockWhenExhausted);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setMaxTotal(maxActive);
        config.setMaxWaitMillis(maxWait);
        config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        return config;
    }

    public static ConfigurationBuilder create() {
        return new ConfigurationBuilder();
    }

    public static class ConfigurationBuilder {
        private Configuration configuration = new Configuration();

        public ConfigurationBuilder withClusterId(String clusterId) {
            Preconditions.checkArgument(clusterId != null && !"".equals(clusterId));
            configuration.setVolName(clusterId);
            return this;
        }

        public ConfigurationBuilder withMaster(String master) {
            Preconditions.checkArgument(master != null, "Invalid master:%s", master);
            configuration.setMaster(master);
            return this;
        }

        public ConfigurationBuilder setMaxRetry(int maxRetry) {
            Preconditions.checkArgument(maxRetry >= 0, "MaxRetry should >= 0");
            configuration.setMaxRetry(maxRetry);
            return this;
        }

        public ConfigurationBuilder withBlockMode(boolean block) {
            configuration.setBlockWhenExhausted(block);
            return this;
        }

        public ConfigurationBuilder withMaxActive(int maxActive) {
            Preconditions.checkArgument(maxActive >= 0, "MaxActive should >= 0");
            configuration.setMaxActive(maxActive);
            return this;
        }

        public ConfigurationBuilder withMaxIdle(int maxIdle) {
            Preconditions.checkArgument(maxIdle >= 0, "MaxIdle should >= 0");
            configuration.setMaxIdle(maxIdle);
            return this;
        }

        public Configuration build() {
            return configuration;
        }
    }
}
