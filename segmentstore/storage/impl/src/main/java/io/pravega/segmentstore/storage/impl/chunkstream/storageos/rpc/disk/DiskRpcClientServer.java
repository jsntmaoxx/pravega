package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcNettyServer;

public abstract class DiskRpcClientServer<ResponseMessage> extends RpcNettyServer<ResponseMessage> {

    @Override
    public void start() throws InterruptedException {
        throw new UnsupportedOperationException("DiskRpcClientServer is not support init");
    }

    @Override
    public void join() throws InterruptedException {
        throw new UnsupportedOperationException("DiskRpcClientServer is not support join");
    }

}
