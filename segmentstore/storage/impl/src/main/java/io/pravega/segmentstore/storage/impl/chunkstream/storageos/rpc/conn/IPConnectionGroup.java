package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;

public abstract class IPConnectionGroup<ResponseMessage> extends ConnectionGroup<ResponseMessage> {
    protected final String nodeId;
    protected final String remoteIp;
    protected final int remotePort;

    public IPConnectionGroup(RpcServer<ResponseMessage> rpcServer, String nodeId, String remoteIp, int remotePort, int connectionNumber) {
        super(rpcServer, connectionNumber);
        this.nodeId = nodeId;
        this.remoteIp = remoteIp;
        this.remotePort = remotePort;
    }

    @Override
    protected String remoteName() {
        return remoteIp + ":" + remotePort + " node " + nodeId;
    }
}
