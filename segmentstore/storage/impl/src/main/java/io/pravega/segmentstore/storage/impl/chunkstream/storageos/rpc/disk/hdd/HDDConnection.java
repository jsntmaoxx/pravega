package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.IPNettyConnection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class HDDConnection extends IPNettyConnection<HDDMessage> {
    private static final Logger log = LoggerFactory.getLogger(HDDConnection.class);
    private static final Duration HDDConnectionSendByteBuffer = Metrics.makeMetric("HDDConnection.bytebuffer.sendRequest.duration", Duration.class);
    private static final Duration HDDConnectionRpcDuration = Metrics.makeMetric("HDDConnection.rpc.duration", Duration.class);
    protected final Map<String, CompletableFuture<HDDMessage>> responseMap = new ConcurrentHashMap<>(1024);

    public HDDConnection(RpcServer<HDDMessage> rpcServer, String host, int port) {
        super(rpcServer, host, port);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected boolean storeRequest(String requestId, CompletableFuture<HDDMessage> retFuture) {
        if (responseMap.putIfAbsent(requestId, retFuture) != null) {
            log().error("request {} exist in connection {}=>{}", requestId, channel.localAddress(), channel.remoteAddress());
            retFuture.completeExceptionally(new CSException("request " + requestId + " exist"));
            return false;
        }
        return true;
    }

    @Override
    protected void markSendComplete(long createTime) {
        HDDConnectionSendByteBuffer.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    protected void markSendResponseComplete(long createTime) {
        throw new UnsupportedOperationException("HDDConnection is not support markSendResponseComplete");
    }

    @Override
    protected void markReceiveResponse(long createTime) {
        HDDConnectionRpcDuration.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    public void completeWithException(Throwable cause) {
        synchronized (this) {
            for (var f : responseMap.values()) {
                f.completeExceptionally(cause);
            }
            responseMap.clear();
        }
    }

    @Override
    public void complete(HDDMessage message) {
        var f = responseMap.remove(message.requestId());
        if (f == null) {
            log().error("can not find complete future of response {} success {}",
                        message.requestId(), message.success());
            return;
        }
        markReceiveResponse(message.requestCreateNano());
        f.complete(message);
    }

    @Override
    public void completeOnSendFailure(String requestId, Throwable cause) {
        var f = responseMap.remove(requestId);
        if (f == null) {
            log().error("can not find complete future of response {} when encounter send failure",
                        requestId, cause);
            return;
        }
        f.completeExceptionally(cause);
    }
}
