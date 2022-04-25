package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.ConnectionGroup;
import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public abstract class RpcNettyServer<ResponseMessage> implements RpcServer<ResponseMessage> {
    protected final Map<String, Map<Integer, ConnectionGroup<ResponseMessage>>> deviceConnGroupMap = new ConcurrentHashMap<>();

    @Override
    public void shutdown() {
        for (var subMap : deviceConnGroupMap.values()) {
            for (var group : subMap.values()) {
                group.shutdown();
            }
        }
    }

    public CompletableFuture<? extends ResponseMessage> sendRequest(String nodeIp, int port, String requestId, long createTime, ByteBuf buf) throws CSException {
        Connection<ResponseMessage> conn = null;
        try {
            conn = connectionGroup(nodeIp, port).lockOneConnection();
            if (conn == null) {
                var f = new CompletableFuture<ResponseMessage>();
                f.completeExceptionally(new CSException("request " + requestId + " can not set up connection to " + nodeIp + ":" + port));
                return f;
            }
            return conn.sendRequest(requestId, createTime, buf);
        } finally {
            if (conn != null) {
                conn.unlock();
            }
        }
    }

    public CompletableFuture<? extends ResponseMessage> sendLocalRequest(String requestId, long createTime, ByteBuf requestBuf, ByteBuf payloadBuf) {
        Connection<ResponseMessage> conn = null;
        try {
            conn = localConnectionGroup().lockOneConnection();
            if (conn == null) {
                var f = new CompletableFuture<ResponseMessage>();
                f.completeExceptionally(new CSException("request " + requestId + " can not set up location connection"));
                return f;
            }
            return conn.sendRequest(requestId, createTime, requestBuf, payloadBuf);
        } finally {
            if (conn != null) {
                conn.unlock();
            }
        }
    }

    public void sendResponse(String responseId, String nodeIp, int port, long createTime, ByteBuf buf) {
        Connection<ResponseMessage> conn = null;
        try {
            conn = connectionGroup(nodeIp, port).lockOneConnection();
            conn.sendResponse(responseId, createTime, buf);
        } finally {
            if (conn != null) {
                conn.unlock();
            }
        }
    }

    protected abstract ConnectionGroup<ResponseMessage> connectionGroup(String nodeIp, int port);

    protected abstract ConnectionGroup<ResponseMessage> localConnectionGroup();
}
