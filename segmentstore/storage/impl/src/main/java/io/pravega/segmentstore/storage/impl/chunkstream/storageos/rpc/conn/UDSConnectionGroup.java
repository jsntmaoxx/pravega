package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;

public abstract class UDSConnectionGroup<ResponseMessage> extends ConnectionGroup<ResponseMessage> {
    protected final String udsPath;

    public UDSConnectionGroup(RpcServer<ResponseMessage> rpcServer,
                              String udsPath,
                              int connectionNumber) {
        super(rpcServer, connectionNumber);
        this.udsPath = udsPath;
    }

    @Override
    protected String remoteName() {
        return udsPath;
    }
}