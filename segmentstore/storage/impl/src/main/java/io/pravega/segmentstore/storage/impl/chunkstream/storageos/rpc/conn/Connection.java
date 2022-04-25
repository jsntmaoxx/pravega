package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public interface Connection<ResponseMessage> {
    String name();

    int port();

    boolean connect();

    /*
        Single thread write
         */
    CompletableFuture<ResponseMessage> sendRequest(String requestId, long createTime, ByteBuf buf);

    CompletableFuture<ResponseMessage> sendRequest(String requestId, long createTime, ByteBuf requestBuf, ByteBuf payloadBuf);

    void sendResponse(String responseId, long createTime, ByteBuf buf);

    boolean lock();

    void unlock();

    void completeWithException(Throwable cause);

    boolean isActive();

    boolean isOpen();

    void complete(ResponseMessage msg);

    void completeOnSendFailure(String requestId, Throwable e);

    void shutdown();
}
