package com.jd.cfs.selector;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jd.cfs.dataserver.DataServer;
import com.jd.cfs.dataserver.DataPartition;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.jd.cfs.common.Constants.UNDERLINE_SEPARATOR;


public class ReadSelect {
    private Set<DataServer> sameZone = Sets.newHashSet();
    private Set<DataServer> sameRegion = Sets.newHashSet();
    private Set<DataServer> others = Sets.newHashSet();

    private DataPartition dataPartition;
    private String localZone;
    private String localRegion;

    public ReadSelect(DataPartition dataPartition, String zone) {
        this.dataPartition = dataPartition;
        this.localZone = zone;
        this.localRegion = zone.split(UNDERLINE_SEPARATOR)[0];
        classify();
    }

    private void classify() {
        for (DataServer ds : dataPartition.getServers()) {
            if (ds.getZone() == null || "".equals(ds.getZone())) {
                others.add(ds);
                continue;
            }
            if (localZone.equals(ds.getZone())) {
                sameZone.add(ds);
            } else {
                String region = ds.getZone().split(UNDERLINE_SEPARATOR)[0];
                if (localRegion.equals(region)) {
                    sameRegion.add(ds);
                } else {
                    others.add(ds);
                }
            }
        }
    }


    public DataServer select(Set<DataServer> exclude) {
        List<DataServer> select = Lists.newArrayList(Sets.difference(sameZone, exclude));

        if (!select.isEmpty()) {
            return select.get(0);
        }

        select = Lists.newArrayList(Sets.difference(sameRegion, exclude));
        if (!select.isEmpty()) {
            Collections.shuffle(select);
            return select.get(0);
        }

        select = Lists.newArrayList(Sets.difference(others, exclude));
        if (!select.isEmpty()) {
            Collections.shuffle(select);
            return select.get(0);
        }
        return null;
    }
}
