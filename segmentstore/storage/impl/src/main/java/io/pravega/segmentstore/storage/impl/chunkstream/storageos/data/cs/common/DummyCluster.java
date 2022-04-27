package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class DummyCluster extends Cluster {
    private static final Logger log = LoggerFactory.getLogger(DummyCluster.class);

    private final String dataEndpoint = "127.0.0.1";
    private final List<String> nodeIds = new ArrayList<>(1);
    private int rdmaIntIp;

    public DummyCluster() {
        try {
            this.rdmaIntIp = InetAddress.getByName(dataEndpoint).hashCode();
        } catch (UnknownHostException e) {
            log.error("failed to convert data endpoint {} to rdma int ip", this.dataEndpoint);
            this.rdmaIntIp = 0;
        }
        nodeIds.add(UUID.randomUUID().toString());
    }

    @Override
    public String currentVdc() {
        return "vdc";
    }

    @Override
    public String currentCos() {
        return "cos";
    }

    @Override
    public String defaultRepGroup() {
        return "rg";
    }

    @Override
    public String currentNodeId() {
        return dataEndpoint;
    }

    @Override
    public String currentDataIp() {
        return dataEndpoint;
    }

    @Override
    public String deviceIdToDataIp(String deviceId) {
        return dataEndpoint;
    }

    @Override
    public int deviceIdToRdmaIntIp(String device) {
        return rdmaIntIp;
    }

    @Override
    public String ownerIp(DTType dtType, DTLevel dtLevel, int dtIndex) {
        return dataEndpoint;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void init() {
    }

    @Override
    public String dataEndpoint() {
        return dataEndpoint;
    }

    @Override
    public String deviceIdToDDBServerIP(UUID device) {
        return dataEndpoint;
    }

    @Override
    public List<String> DDBServerIPs() {
        return Collections.singletonList(dataEndpoint);
    }

}
