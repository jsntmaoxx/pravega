package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import com.google.protobuf.GeneratedMessage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public abstract class DTClient {
    private static final AtomicLong dtIdGen = new AtomicLong(0);
    protected final Cluster cluster;
    protected final Map<String, CompletableFuture<FileOperationsPayload>> responseMap = new ConcurrentHashMap<>(1024);
    private final DTRpcServer rpcServer;
    private final String randomRequestTag = Integer.toString(ThreadLocalRandom.current().nextInt(1, 65536));

    public DTClient(DTRpcServer rpcServer, Cluster cluster) {
        this.rpcServer = rpcServer;
        this.cluster = cluster;
    }

    protected CompletableFuture<? extends FileOperationsPayload> sendRequest(DTType dtType,
                                                                             DTLevel dtLevel,
                                                                             int dtHash,
                                                                             FileOperationsPayloads.CommandType commandType,
                                                                             GeneratedMessage request) throws CSException, IOException {
        var createNano = System.nanoTime();
        var requestId = "r-" + dtType + "-" + dtIdGen.incrementAndGet() + "-" + randomRequestTag;
        return rpcServer.sendRequest(cluster.ownerIp(dtType, dtLevel, dtHash),
                                     dtServerPort(),
                                     requestId,
                                     createNano,
                                     DTMessageBuilder.makeByteBuf(DTMessageBuilder.makeFileOperationPayloads(cluster, requestId, commandType, dtType, dtLevel, dtHash, request), createNano));
    }

    protected abstract int dtServerPort();

}
