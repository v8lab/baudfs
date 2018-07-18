package com.jd.cfs;

import org.junit.Before;

/**
 * Created by zhuhongyin
 * Created on 2018/7/2.
 */
public class BaseTest {
    BaudFSClient cfsClient;
    @Before
    public void createClient(){
        Configuration cfg = new Configuration();
        cfg.setMaster("10.196.31.173:80;10.196.31.141:80");
        cfg.setVolName("intest");
        cfg.setZone("huitian_rack1");
        cfsClient= new BaudFSClient(cfg);
    }
}
