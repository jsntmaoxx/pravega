package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Response;

public abstract class HDDResponse implements HDDMessage {
    protected final Response response;
    protected final long createNano;

    public HDDResponse(Response response) {
        this.response = response;
        createNano = System.nanoTime();
    }

    @Override
    public boolean success() {
        return response.getStatus() == Response.Status.SUCCESS;
    }

    @Override
    public String requestId() {
        return response.getRequestId();
    }

    @Override
    public String errorMessage() {
        return response.getErrorMessage();
    }

    public long responseCreateNano() {
        return response.getCreateTime();
    }

    @Override
    public long requestCreateNano() {
        return response.getRequestTime();
    }

    @Override
    public long responseReceiveNano() {
        return createNano;
    }
}
