package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NettyConnection<ResponseMessage> implements Connection<ResponseMessage> {
    protected final RpcServer<ResponseMessage> rpcServer;
    protected final AtomicBoolean lock = new AtomicBoolean(false);
    protected Channel channel;
    protected String name;

    public NettyConnection(RpcServer<ResponseMessage> rpcServer) {
        this.rpcServer = rpcServer;
    }

    /*
        Single thread write
         */
    @Override
    public CompletableFuture<ResponseMessage> sendRequest(String requestId, long createTime, ByteBuf buf) {
        return sendRequest(requestId, createTime, buf, null);
    }

    @Override
    public CompletableFuture<ResponseMessage> sendRequest(String requestId, long createTime, ByteBuf requestBuf, ByteBuf payloadBuf) {
        var retFuture = new CompletableFuture<ResponseMessage>();
        if (channel == null || !channel.isOpen()) {
            log().error("can not connect to {}", name());
            retFuture.completeExceptionally(new CSException("can not connect to " + name()));
            return retFuture;
        }
        if (!storeRequest(requestId, retFuture)) {
            retFuture.completeExceptionally(new CSException("failed store request " + requestId));
            return retFuture;
        }
        var buf = payloadBuf == null ? requestBuf : Unpooled.wrappedBuffer(requestBuf, payloadBuf);
        channel.writeAndFlush(buf, new DefaultChannelPromise(channel).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                var f = (ChannelFuture) future;
                if (f.cause() != null) {
                    log().error("r-{} write and flush buf left bytes {} failed", requestId, buf.readableBytes(), f.cause());
                    completeOnSendFailure(requestId, f.cause());
                } else if (!f.isSuccess()) {
                    log().error("r-{} write and flush buf {} is not success", requestId, buf.readableBytes());
                    completeOnSendFailure(requestId, new CSException("send request failed"));
                }
                markSendComplete(createTime);
            }
        }));
        return retFuture;
    }

    @Override
    public void sendResponse(String responseId, long createTime, ByteBuf buf) {
        channel.writeAndFlush(buf, new DefaultChannelPromise(channel).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                var f = (ChannelFuture) future;
                if (f.cause() != null) {
                    log().error("write and flush response r-{} buf {} failed", responseId, buf.readableBytes(), f.cause());
                } else if (!f.isSuccess()) {
                    log().error("write and flush response r-{} buf {} is not success", responseId, buf.readableBytes());
                }
                markSendResponseComplete(createTime);
            }
        }));
    }

    protected abstract Logger log();

    protected abstract boolean storeRequest(String requestId, CompletableFuture<ResponseMessage> retFuture);

    protected abstract void markSendComplete(long createTime);

    protected abstract void markSendResponseComplete(long createTime);

    protected abstract void markReceiveResponse(long requestCreateNano);

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean lock() {
        return lock.compareAndSet(false, true);
    }

    @Override
    public void unlock() {
        lock.set(false);
    }

    @Override
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    @Override
    public void shutdown() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close().sync();
                log().info("close connection {}", name);
            }
        } catch (InterruptedException e) {
            log().error("close connection {} failed", name, e);
        }
    }

}
