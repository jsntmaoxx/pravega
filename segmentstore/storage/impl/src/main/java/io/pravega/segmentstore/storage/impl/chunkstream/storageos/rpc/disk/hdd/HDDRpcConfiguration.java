package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;

public class HDDRpcConfiguration extends RpcConfiguration {
    protected int connectionNumber = 1 << 4; // must power of 2
    protected int nettyServiceEventLoopNumber = 4;
    protected int nettyServiceExecutorWorkerNumber = 2;

    @Override
    public int connectionNumber() {
        return connectionNumber;
    }

    @Override
    public void connectionNumber(int connectionNumber) {
        this.connectionNumber = connectionNumber;
    }

    @Override
    public int serverPort() {
        return Cluster.csInternalPort;
    }

    @Override
    public int nettyServiceEventLoopNumber() {
        return nettyServiceEventLoopNumber;
    }

    @Override
    public int nettyServiceExecutorWorkerNumber() {
        return nettyServiceExecutorWorkerNumber;
    }
}
