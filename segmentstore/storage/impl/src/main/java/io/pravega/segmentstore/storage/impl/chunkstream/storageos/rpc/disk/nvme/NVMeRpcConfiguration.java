package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;

public class NVMeRpcConfiguration extends RpcConfiguration {
    private static final boolean enableShareMemByHugePage = false;
    // share mem
    private static final boolean enableSharedMemConnection = true;
    private static final int sharedMemPageNumberPerfConnection = 16;
    private static final int maxShareMemConnectionNumber = 256;
    private static final int shareMemTaskExecuteWorkerNumber = 1;
    private static final int shmHugePageDiskConnectionNumber = 1 << 7; // must power
    protected int nettyServiceEventLoopNumber = 8;
    protected int nettyServiceExecutorWorkerNumber = 2;
    private int connectionNumber = 1 << 5; // must power of 2
    // debug
    private boolean skipShareMemDataCopy = false;

    public static boolean enableShareMemByHugePage() {
        return enableShareMemByHugePage;
    }

    @Override
    public int connectionNumber() {
        return enableSharedMemConnection && enableShareMemByHugePage ? shmHugePageDiskConnectionNumber : connectionNumber;
    }

    @Override
    public void connectionNumber(int connectionNumber) {
        this.connectionNumber = connectionNumber;
    }

    @Override
    public int serverPort() {
        return Cluster.csInternalPort;
    }

    public boolean enableSharedMemConnection() {
        return enableSharedMemConnection;
    }

    public int sharedMemPageNumberPerfConnection() {
        return sharedMemPageNumberPerfConnection;
    }

    public int maxShareMemConnectionNumber() {
        return maxShareMemConnectionNumber;
    }

    public int shareMemTaskExecuteWorkerNumber() {
        return shareMemTaskExecuteWorkerNumber;
    }

    @Override
    public boolean skipShareMemDataCopy() {
        return skipShareMemDataCopy;
    }

    @Override
    public void skipShareMemDataCopy(boolean skipShareMemDataCopy) {
        this.skipShareMemDataCopy = skipShareMemDataCopy;
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
