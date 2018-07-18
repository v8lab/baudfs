package com.jd.cfs.dataserver;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.jd.cfs.selector.ReadSelect;

import java.util.List;
import java.util.Set;

public class DataPartition implements Comparable<Integer> {
    @JsonProperty("PartitionID")
    private Integer partitionId;
    @JsonProperty("Status")
    private State state;
    @JsonProperty("Hosts")
    private List<DataServer> servers;

    private ReadSelect select;

    public Integer getPartitionId() {
        return this.partitionId;
    }


    public State getState() {
        return this.state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public List<DataServer> getServers() {
        return this.servers;
    }


    public enum State {
        UNAVAILABLE(-1), READABLE(1), WRITEABLE(2);
        private int state;

        State(int state) {
            this.state = state;
        }

        public int getState() {
            return this.state;
        }

        @JsonCreator
        public static State forValue(int value) {
            switch (value) {
                case -1:
                    return State.UNAVAILABLE;
                case 1:
                    return State.READABLE;
                case 2:
                    return State.WRITEABLE;
                default:
                    throw new IllegalArgumentException("Illegal argument :" + value);
            }
        }
    }


    public DataServer read(String zone, Set<DataServer> exclude) {
        if (select == null) {
            select = new ReadSelect(this, zone);
        }
        return select.select(exclude);
    }

    public DataServer getMaster() {
        return servers.get(0);
    }


    @Override
    public String toString() {
        return "DataPartition{" +
                "partitionId=" + partitionId +
                ", state=" + state +
                ", servers=" + servers +
                '}';
    }

    public String replication() {
        return Joiner.on("/").join(servers.subList(1, servers.size())) + "/";
    }

    @Override
    public int compareTo(Integer o) {
        return this.partitionId.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataPartition dataPartition = (DataPartition) o;

        return this.compareTo(dataPartition.partitionId) == 0;
    }

    @Override
    public int hashCode() {
        int result = partitionId != null ? partitionId.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (servers != null ? servers.hashCode() : 0);
        result = 31 * result + (select != null ? select.hashCode() : 0);
        return result;
    }
}