package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc;

public abstract class RpcConfiguration {
    protected long rpcTimeoutSeconds = 20;

    @Override
    public String toString() {
        return new StringBuilder(1024).append("\n")
                                      .append("connectionNumber         ").append(connectionNumber()).append("\n")
                                      .toString();
    }

    public abstract int nettyServiceEventLoopNumber();

    public abstract int nettyServiceExecutorWorkerNumber();

    public abstract int connectionNumber();

    public abstract void connectionNumber(int connectionNumber);

    public abstract int serverPort();

    public boolean skipShareMemDataCopy() {
        return false;
    }

    public void skipShareMemDataCopy(boolean skipShareMemDataCopy) {
    }

    public long rpcTimeoutSeconds() {
        return rpcTimeoutSeconds;
    }
}
