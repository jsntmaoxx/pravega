package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.UDSNettyConnection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class NVMeConnection extends UDSNettyConnection<NVMeMessage> {
    private static final Logger log = LoggerFactory.getLogger(NVMeConnection.class);
    private static final Duration NVMeConnectionSendByteBuffer = Metrics.makeMetric("NVMeConnection.bytebuffer.sendRequest.duration", Duration.class);
    protected final Map<String, CompletableFuture<NVMeMessage>> responseMap = new ConcurrentHashMap<>(1024);

    public NVMeConnection(RpcServer<NVMeMessage> rpcServer, String udsPath) {
        super(rpcServer, udsPath);
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
    public void complete(NVMeMessage message) {
        var f = responseMap.remove(message.requestId());
        if (f == null) {
            log().error("can not find complete future of response {} success {}",
                        message.requestId(), message.success());
            return;
        }
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

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected boolean storeRequest(String requestId, CompletableFuture<NVMeMessage> retFuture) {
        if (responseMap.putIfAbsent(requestId, retFuture) != null) {
            log().error("request {} exist in uds connection =>{}", requestId, channel.remoteAddress());
            retFuture.completeExceptionally(new CSException("request " + requestId + " exist"));
            return false;
        }
        return true;
    }

    @Override
    protected void markSendComplete(long createTime) {
        NVMeConnectionSendByteBuffer.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    protected void markSendResponseComplete(long createTime) {
        throw new UnsupportedOperationException("NVMeConnection is not support markSendResponseComplete");
    }

    @Override
    protected void markReceiveResponse(long requestCreateNano) {
    }
}
