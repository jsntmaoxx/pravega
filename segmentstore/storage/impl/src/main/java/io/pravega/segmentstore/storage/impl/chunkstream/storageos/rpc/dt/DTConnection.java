package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.IPNettyConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DTConnection extends IPNettyConnection<FileOperationsPayload> {
    private static final Logger log = LoggerFactory.getLogger(DTConnection.class);
    private static final Duration DTConnectionSendRequest = Metrics.makeMetric("DTConnection.sendRequest.duration", Duration.class);
    private static final Duration DTConnectionSendResponse = Metrics.makeMetric("DTConnection.sendResponse.duration", Duration.class);
    private static final Duration DTConnectionRpcDuration = Metrics.makeMetric("DTConnection.rpc.duration", Duration.class);
    private final DTRequestContainer requestContainer;

    public DTConnection(RpcServer<FileOperationsPayload> rpcServer, String host, int port, DTRequestContainer requestContainer) {
        super(rpcServer, host, port);
        this.requestContainer = requestContainer;
        this.name = "->" + host + ":" + port;
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected boolean storeRequest(String requestId, CompletableFuture<FileOperationsPayload> retFuture) {
        return requestContainer.storeRequest(requestId, retFuture, name);
    }

    @Override
    protected void markSendComplete(long createTime) {
        DTConnectionSendRequest.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    protected void markSendResponseComplete(long createTime) {
        DTConnectionSendResponse.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    protected void markReceiveResponse(long createTime) {
        DTConnectionRpcDuration.updateNanoSecond(System.nanoTime() - createTime);
    }

    @Override
    public void completeWithException(Throwable cause) {
        // do nothing
    }

    @Override
    public void complete(FileOperationsPayload msg) {
        requestContainer.complete(msg);
    }

    @Override
    public void completeOnSendFailure(String requestId, Throwable e) {
        requestContainer.completeOnSendFailure(requestId, e);
    }
}
