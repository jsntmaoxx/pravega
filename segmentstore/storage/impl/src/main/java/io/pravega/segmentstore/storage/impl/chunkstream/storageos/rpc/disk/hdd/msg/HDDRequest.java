package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Request;
import io.netty.buffer.ByteBuf;

public abstract class HDDRequest implements HDDMessage {
    protected final Request request;

    public HDDRequest(Request request) {
        this.request = request;
    }

    @Override
    public String requestId() {
        return request.getRequestId();
    }

    @Override
    public long requestCreateNano() {
        return request.getCreateTime();
    }

    @Override
    public boolean success() {
        throw new UnsupportedOperationException("DiskRequest is not support success");
    }

    @Override
    public long responseReceiveNano() {
        throw new UnsupportedOperationException("DiskRequest is not support responseReceiveNano");
    }

    @Override
    public String errorMessage() {
        throw new UnsupportedOperationException("DiskRequest is not support errorMessage");
    }

    public abstract StorageServerMessages.Response process();

    public abstract ByteBuf sendBuffer();
}
