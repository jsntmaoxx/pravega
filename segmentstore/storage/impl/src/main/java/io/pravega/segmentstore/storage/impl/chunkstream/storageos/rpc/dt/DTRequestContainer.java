package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import java.util.concurrent.CompletableFuture;

public interface DTRequestContainer<ResponseMessage> {
    boolean storeRequest(String requestId, CompletableFuture<ResponseMessage> retFuture, String connectionName);

    void complete(ResponseMessage msg);

    void completeOnSendFailure(String requestId, Throwable e);
}
