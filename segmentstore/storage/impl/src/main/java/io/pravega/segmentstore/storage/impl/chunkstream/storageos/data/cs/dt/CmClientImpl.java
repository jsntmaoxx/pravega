package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRecords;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel.Level1;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType.CT;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.CommandType.*;

public class CmClientImpl extends DTClient implements CmClient {
    private static final Logger log = LoggerFactory.getLogger(CmClientImpl.class);
    private static final Duration cmCreateChunkDuration = Metrics.makeMetric("cm.chunk.create.duration", Duration.class);
    private static final Duration cmQueryChunkDuration = Metrics.makeMetric("cm.chunk.query.duration", Duration.class);
    private static final Duration cmDeleteChunkDuration = Metrics.makeMetric("cm.chunk.delete.duration", Duration.class);
    private static final Duration cmSealChunkDuration = Metrics.makeMetric("cm.chunk.seal.duration", Duration.class);
    private static final Duration cmCompleteClientECDuration = Metrics.makeMetric("cm.chunk.complete.ec.duration", Duration.class);
    private static final Duration cmListChunkDuration = Metrics.makeMetric("cm.chunk.list.duration", Duration.class);
    private final CSConfiguration csConfig;
    private final DiskClient<DiskMessage> diskClient;
    private final ChunkCache chunkCache;
    private final AtomicLong minTag = new AtomicLong(0);

    public CmClientImpl(CSConfiguration csConfig, DiskClient<DiskMessage> diskClient, DTRpcServer dtRpcServer, Cluster cluster, ChunkCache chunkCache) {
        super(dtRpcServer, cluster);
        this.csConfig = csConfig;
        this.diskClient = diskClient;
        this.chunkCache = chunkCache;
    }

    @Override
    public CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity, ChunkConfig chunkConfig, String objKey) throws CSException, IOException {
        return createChunk(chunkType, chunkId, indexGranularity, chunkConfig, objKey, null);
    }

    private CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity, ChunkConfig chunkConfig, String objKey, Long timestamp) throws CSException, IOException {
        CompletableFuture<ChunkObject> createFuture = new CompletableFuture<>();
        var strChunkId = chunkId.toString();
        Long tag = timestamp != null ? timestamp : nextTag();
        CmMessage.ChunkCreateRequest.Builder requestBuilder =
                CmMessage.ChunkCreateRequest.newBuilder()
                                            .setChunkId(strChunkId)
                                            .setDataType(CmMessage.ChunkDataType.REPO)
                                            .setType(CmMessage.ChunkType.LOCAL)
                                            .setUnlimitedSize(false)
                                            .setSkipChunkSequenceNumber(false)
                                            .setWriterNodeId(cluster.currentNodeId())
                                            .setRepGroup(cluster.defaultRepGroup())
                                            .setRepoChunkType(toRepoChunkType(chunkType))
                                            .setMaxWriteBlockSize(CSConfiguration.writeSegmentSize())
                                            .setIndexGranularity(indexGranularity)
                                            .setCustomChunkSize(chunkConfig.chunkSize())
                                            .setEcCodeScheme(chunkConfig.ecSchema().ecCodeScheme)
                                            .setSkipRepoGC(true)
                                            .setTag(tag);
        if (objKey != null) {
            requestBuilder.setOriginalKey(objKey);
        }
        CmMessage.ChunkCreateRequest request = requestBuilder.build();
        var startNano = System.nanoTime();
        sendRequest(CT, Level1, cluster.dtHash(CT, strChunkId), REQUEST_CREATE_CHUNK, request)
                .orTimeout(csConfig.createChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("create chunk {} type {} failed", chunkId, chunkType, t);
                        createFuture.completeExceptionally(t);
                        return;
                    }

                    cmCreateChunkDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("create chunk {} type {} failed, remote process status {}", chunkId, chunkType, r.getRemoteProcessStatus());
                            createFuture.completeExceptionally(new CSException("create chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = CmMessage.ChunkCreateResponse.parseFrom(r.getPayload());
                        if (response.getStatus() != CmMessage.ChunkOperationStatus.SUCCESS) {
                            log.error("create chunk {} type {} failed, response status {}", chunkId, chunkType, response.getStatus());
                            createFuture.completeExceptionally(new CSException("create chunk failed with status " + response.getStatus()));
                            return;
                        }

                        var chunkInfo = response.getChunkInfo();
                        // cj_todo move to cm side
                        if (!chunkInfo.hasIndexGranularity()) {
                            chunkInfo = response.getChunkInfo().toBuilder().setIndexGranularity(indexGranularity).build();
                        }

                        var createTime = chunkInfo.getCreateTime();
                        if (tag != chunkInfo.getTag() || chunkInfo.getStatus() != CmMessage.ChunkStatus.ACTIVE) {
                            log.warn("chunk {} was already exist, overwrite it",
                                     chunkId);
                            deleteChunk(chunkId).whenComplete((r2, t2) -> {
                                if (t2 != null) {
                                    createFuture.completeExceptionally(t2);
                                } else {
                                    try {
                                        createChunk(chunkType, chunkId, indexGranularity, chunkConfig, objKey, tag).whenComplete((r3, t3) -> {
                                            if (t3 != null) {
                                                createFuture.completeExceptionally(t3);
                                            } else {
                                                createFuture.complete(r3);
                                            }
                                        });
                                    } catch (Exception e) {
                                        createFuture.completeExceptionally(e);
                                    }
                                }
                            });
                            return;
                        }

                        ChunkObject chunkObject;
                        switch (chunkType) {
                            default:
                            case Normal:
                                chunkObject = new NormalChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
                                break;
                            case I:
                                chunkObject = new Type1ChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
                                break;
                            case II:
                                chunkObject = new Type2ChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
                                break;
                        }
                        log.info("create chunk {} type {}", chunkObject.chunkIdStr(), chunkType);
                        chunkCache.put(chunkId, chunkObject);
                        createFuture.complete(chunkObject);
                    } catch (Exception e) {
                        log.error("handle create chunk {} type {} response failed", chunkId, chunkType, e);
                        createFuture.completeExceptionally(e);
                    }
                });
        return createFuture;
    }

    @Override
    public CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity) throws IOException, CSException {
        return createChunk(chunkType, chunkId, indexGranularity, csConfig.defaultChunkConfig(), null);
    }

    @Override
    public CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache, ChunkConfig chunkConfig) throws CSException, IOException {
        CompletableFuture<ChunkObject> queryFuture = new CompletableFuture<>();
        var strChunkId = chunkId.toString();
        CmMessage.ChunkQueryRequest request =
                CmMessage.ChunkQueryRequest.newBuilder()
                                           .setChunkId(strChunkId)
                                           .build();
        var startNano = System.nanoTime();
        sendRequest(CT, Level1, cluster.dtHash(CT, strChunkId), REQUEST_QUERY_CHUNK, request)
                .orTimeout(csConfig.createChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("query chunk {} failed", chunkId, t);
                        queryFuture.completeExceptionally(t);
                        return;
                    }

                    cmQueryChunkDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("query chunk {} failed, remote process status {}", chunkId, r.getRemoteProcessStatus());
                            queryFuture.completeExceptionally(new CSException("query chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = CmMessage.ChunkQueryResponse.parseFrom(r.getPayload());
                        if (response.getStatus() == CmMessage.ChunkOperationStatus.SUCCESS) {
                            ChunkObject chunkObject = makeChunkObject(chunkId, response.getChunkInfo(), chunkConfig);
                            if (updateCache) {
                                chunkCache.put(chunkId, chunkObject);
                            }
                            log.info("query chunk {} update cache {}", chunkObject.chunkIdStr(), updateCache);
                            queryFuture.complete(chunkObject);
                        } else if (response.getStatus() == CmMessage.ChunkOperationStatus.ERROR_NO_SUCH_CHUNK) {
                            log.info("query chunk {} is not exist", chunkId);
                            queryFuture.complete(null);
                        } else {
                            log.error("query chunk {} failed, response status {}", chunkId, response.getStatus());
                            queryFuture.completeExceptionally(new CSException("query chunk failed with status " + response.getStatus()));
                        }
                    } catch (Exception e) {
                        log.error("handle query chunk {} response failed", chunkId, e);
                        queryFuture.completeExceptionally(e);
                    }
                });
        return queryFuture;
    }

    @Override
    public CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache) throws IOException, CSException {
        return queryChunk(chunkId, updateCache, csConfig.defaultChunkConfig());
    }

    @Override
    public CompletableFuture<Void> deleteChunk(UUID chunkId) throws CSException, IOException {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        var strChunkId = chunkId.toString();
        CmMessage.ChunkObjectDeleteRequest request =
                CmMessage.ChunkObjectDeleteRequest.newBuilder()
                                                  .setChunkId(strChunkId)
                                                  .setFreeBlockDelaySeconds(csConfig.freeBlockDelaySeconds())
                                                  .build();

        var startNano = System.nanoTime();
        sendRequest(CT, Level1, cluster.dtHash(CT, strChunkId), REQUEST_DELETE_CHUNK_OBJECT, request)
                .orTimeout(csConfig.createChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("delete chunk {} failed", chunkId, t);
                        deleteFuture.completeExceptionally(t);
                        return;
                    }

                    cmDeleteChunkDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("delete chunk {} failed, remote process status {}", chunkId, r.getRemoteProcessStatus());
                            deleteFuture.completeExceptionally(new CSException("delete chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = CmMessage.ChunkObjectDeleteResponse.parseFrom(r.getPayload());
                        if (response.getStatus() != CmMessage.ChunkOperationStatus.SUCCESS &&
                            response.getStatus() != CmMessage.ChunkOperationStatus.CHUNK_NOT_EXIST) {
                            log.error("delete chunk {} failed, response status {}", chunkId, response.getStatus());
                            deleteFuture.completeExceptionally(new CSException("delete chunk failed with status " + response.getStatus()));
                            return;
                        }
                        if (response.getStatus() == CmMessage.ChunkOperationStatus.CHUNK_NOT_EXIST) {
                            log.warn("delete chunk {} not exist, response status {}", chunkId, response.getStatus());
                        } else {
                            log.info("delete chunk {}", chunkId);
                        }
                        chunkCache.remove(chunkId);
                        deleteFuture.complete(null);
                    } catch (Exception e) {
                        log.error("handle delete chunk {} response failed", chunkId, e);
                        deleteFuture.completeExceptionally(e);
                    }
                });
        return deleteFuture;
    }

    @Override
    public CompletableFuture<ChunkObject> sealChunk(ChunkObject chunkObj, int sealLength) throws CSException, IOException {
        CompletableFuture<ChunkObject> sealFuture = new CompletableFuture<>();
        if (chunkObj == null) {
            sealFuture.completeExceptionally(new CSException("null chunk object"));
            return sealFuture;
        }
        CmMessage.ChunkSealRequest request =
                CmMessage.ChunkSealRequest.newBuilder()
                                          .setChunkId(chunkObj.chunkIdStr())
                                          .setLength(sealLength)
                                          .setIsEcInClient(true)
                                          .build();

        var startNano = System.nanoTime();
        sendRequest(CT, Level1, cluster.dtHash(CT, chunkObj.chunkIdStr()), REQUEST_SEAL_CHUNK, request)
                .orTimeout(csConfig.createChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("seal chunk {} length {} failed", chunkObj.chunkIdStr(), sealLength, t);
                        sealFuture.completeExceptionally(t);
                        return;
                    }

                    cmSealChunkDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("seal chunk {} length {} failed, remote process status {}", chunkObj.chunkIdStr(), sealLength, r.getRemoteProcessStatus());
                            sealFuture.completeExceptionally(new CSException("seal chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = CmMessage.ChunkSealResponse.parseFrom(r.getPayload());
                        if (response.getStatus() != CmMessage.ChunkOperationStatus.SUCCESS) {
                            log.error("seal chunk {} length {} failed, response status {}", chunkObj.chunkIdStr(), sealLength, response.getStatus());
                            sealFuture.completeExceptionally(new CSException("seal chunk failed with status " + response.getStatus()));
                            return;
                        }

                        // cj_todo delete the IndexGranularity code after change ECS
                        var newChunkInfo = response.getChunkInfo();
                        if (!newChunkInfo.hasIndexGranularity()) {
                            newChunkInfo = newChunkInfo.toBuilder().setIndexGranularity(chunkObj.chunkInfo().getIndexGranularity()).build();
                        }
                        chunkObj.chunkInfo(newChunkInfo);
                        log.info("seal chunk {} length {}", chunkObj.chunkIdStr(), sealLength);
                        sealFuture.complete(chunkObj);
                    } catch (Exception e) {
                        log.error("handle seal chunk {} length {} response failed", chunkObj.chunkIdStr(), sealLength, e);
                    }
                });
        return sealFuture;
    }

    @Override
    public CompletableFuture<ChunkObject> completeClientEC(ChunkObject chunkObj, int sealLength, CmMessage.Copy ecCopy) throws CSException, IOException {
        CompletableFuture<ChunkObject> completeEcFuture = new CompletableFuture<>();
        if (ecCopy == null) {
            completeEcFuture.completeExceptionally(new CSException("null chunk ec copy"));
            return completeEcFuture;
        }
        CmMessage.ClientEcEncodeCompletionRequest request =
                CmMessage.ClientEcEncodeCompletionRequest.newBuilder()
                                               .setChunkId(chunkObj.chunkIdStr())
                                               .setEcCopy(ecCopy)
                                               .setSealLength(sealLength)
                                               .build();

        var startNano = System.nanoTime();
        sendRequest(CT, Level1, cluster.dtHash(CT, chunkObj.chunkIdStr()), REQUEST_CLIENT_EC_COMPLETE, request)
                .orTimeout(csConfig.createChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("complete client ec on chunk {} length {} failed", chunkObj.chunkIdStr(), sealLength, t);
                        completeEcFuture.completeExceptionally(t);
                        return;
                    }

                    cmCompleteClientECDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("complete client ec on chunk {} length {} failed, remote process status {}", chunkObj.chunkIdStr(), sealLength, r.getRemoteProcessStatus());
                            completeEcFuture.completeExceptionally(new CSException("complete client ec on chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = CmMessage.ClientEcEncodeCompletionResponse.parseFrom(r.getPayload());
                        if (response.getStatus() != CmMessage.ChunkOperationStatus.SUCCESS) {
                            log.error("complete client ec on chunk {} length {} failed, response status {}", chunkObj.chunkIdStr(), sealLength, response.getStatus());
                            completeEcFuture.completeExceptionally(new CSException("complete client ec on chunk failed with status " + response.getStatus()));
                            return;
                        }

                        var newChunkInfo = response.getChunkInfo();
                        chunkObj.chunkInfo(newChunkInfo);
                        log.info("complete client ec on chunk {} length {}", chunkObj.chunkIdStr(), sealLength);
                        completeEcFuture.complete(chunkObj);
                    } catch (Exception e) {
                        log.error("handle seal chunk {} length {} response failed", chunkObj.chunkIdStr(), sealLength, e);
                        completeEcFuture.completeExceptionally(e);
                    }
                });
        return completeEcFuture;
    }

    @Override
    public CompletableFuture<ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey>>
    listChunks(int ctIndex, UUID startAfter, UUID endBefore, int maxKeys, String prefix) throws CSException, IOException {
        CompletableFuture<ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey>> queryFuture = new CompletableFuture<>();
        DTRecords.DirectoryListKVRequest request =
                DTRecords.DirectoryListKVRequest.newBuilder()
                                                .setSchemaToken(makeChunkKey(startAfter))
                                                .setSchemaListEndKey(makeChunkKey(endBefore))
                                                .setMaxKeys(maxKeys)
                                                .setDtIndex(ctIndex)
                                                .setIncludeUncommitted(false)
                                                .build();
        if (prefix != null) {
            request.toBuilder().setSchemaListPrefix(makeChunkKey(prefix));
        }

        var startNano = System.nanoTime();
        sendRequest(CT, Level1, ctIndex, REQUEST_LIST_ENTRY, request)
                .orTimeout(csConfig.listChunkTimeoutSeconds(), TimeUnit.SECONDS)
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("list chunk on CT {} ({}, {}) failed", ctIndex, startAfter, endBefore, t);
                        queryFuture.completeExceptionally(t);
                        return;
                    }

                    cmListChunkDuration.updateNanoSecond(System.nanoTime() - startNano);

                    try {
                        if (r.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("list chunk on CT {} ({}, {}) failed, remote process status {}", ctIndex, startAfter, endBefore, r.getRemoteProcessStatus());
                            queryFuture.completeExceptionally(new CSException("query chunk failed with remote process status " + r.getRemoteProcessStatus()));
                            return;
                        }

                        var response = DTRecords.DirectoryListKVResponse.parseFrom(r.getPayload());
                        if (response.getStatus() == DTRecords.DirectoryOperationStatus.SUCCESS) {
                            log.info("list chunk on CT {} ({}, {}) return {} records truncate {}", ctIndex, startAfter, endBefore, response.getEntriesCount(), !response.hasSchemaToken());
                            queryFuture.complete(ImmutablePair.of(response.getEntriesList(), response.hasSchemaToken() ? response.getSchemaToken() : null));
                        } else if (response.getStatus() == DTRecords.DirectoryOperationStatus.ERROR_KEY_NOT_FOUND) {
                            log.warn("list chunk on CT {} ({}, {}) failed with status {}", ctIndex, startAfter, endBefore, response.getStatus());
                            queryFuture.complete(ImmutablePair.of(Collections.emptyList(), null));
                        } else {
                            log.info("list chunk on CT {} ({}, {}) failed with status {}", ctIndex, startAfter, endBefore, response.getStatus());
                            queryFuture.completeExceptionally(new CSException("list chunk on CT " + ctIndex + " failed with status " + response.getStatus()));
                        }
                    } catch (Exception e) {
                        log.error("list chunk on CT {} ({}, {}) failed", ctIndex, startAfter, endBefore, e);
                        queryFuture.completeExceptionally(e);
                    }
                });
        return queryFuture;
    }

    @Override
    public CompletableFuture<StreamProto.StreamValue> queryStream(String streamName) {
        throw new UnsupportedOperationException("CmClientImpl is not support queryStream");
    }

    @Override
    public CompletableFuture<StreamProto.StreamValue> createStreamIfAbsent(String streamName) {
        throw new UnsupportedOperationException("CmClientImpl is not support createStreamIfAbsent");
    }

    @Override
    public CompletableFuture<List<StreamProto.StreamDataValue>> listStreamDataValue(String streamName, long startSequence, int max) {
        throw new UnsupportedOperationException("CmClientImpl is not support listStreamDataValue");
    }

    @Override
    public CompletableFuture<List<StreamChunkObject>> createStreamChunk(String streamId, List<UUID> chunkIds, ChunkConfig chunkConfig) throws IOException {
        throw new UnsupportedOperationException("CmClientImpl is not support createStreamChunk");
    }


    @Override
    public CompletableFuture<Void> truncateStream(String streamId, StreamProto.StreamPosition streamPosition) {
        throw new UnsupportedOperationException("CmClientImpl is not support truncateStream");
    }

    private SchemaKeyRecords.SchemaKey makeChunkKey(UUID chunkId) {
        return SchemaKeyRecords.SchemaKey
                .newBuilder().setType(SchemaKeyRecords.SchemaKeyType.CHUNK)
                .setUserKey(SchemaKeyRecords.ChunkKey
                                    .newBuilder()
                                    .setChunkId(chunkId.toString())
//                                    .setChunkIdUuid(SchemaKeyRecords.Uuid
//                                                            .newBuilder()
//                                                            .setHighBytes(chunkId.getMostSignificantBits())
//                                                            .setLowBytes(chunkId.getLeastSignificantBits())
//                                                            .build())
                                    .build()
                                    .toByteString())
                .build();
    }

    private SchemaKeyRecords.SchemaKey makeChunkKey(String prefix) {
        return SchemaKeyRecords.SchemaKey
                .newBuilder().setType(SchemaKeyRecords.SchemaKeyType.CHUNK)
                .setUserKey(SchemaKeyRecords.ChunkKey
                                    .newBuilder()
                                    .setChunkId(prefix)
                                    .build()
                                    .toByteString())
                .build();
    }

    private CmMessage.RepoChunkType toRepoChunkType(ChunkType chunkType) throws CSException {
        switch (chunkType) {
            case Normal:
            case I:
                return CmMessage.RepoChunkType.TYPE_I;
            case II:
                return CmMessage.RepoChunkType.TYPE_II;
            case III:
            case IV:
            default:
                log.error("can not convert chunkType {} to CmMessage.RepoChunkType", chunkType);
                throw new CSException("failed to convert to CmMessage.RepoChunkType");
        }
    }

    private ChunkObject makeChunkObject(UUID chunkId, CmMessage.ChunkInfo chunkInfo, ChunkConfig chunkConfig) throws CSException {
        ChunkObject chunkObject;
        if (chunkInfo.hasRepoChunkType()) {
            if (chunkInfo.getRepoChunkType() == CmMessage.RepoChunkType.TYPE_I) {
                chunkObject = new Type1ChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
            } else {
                chunkObject = new Type2ChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
            }
        } else if (chunkInfo.getDataType() == CmMessage.ChunkDataType.REPO) {
            chunkObject = new NormalChunkObject(chunkId, chunkInfo, diskClient, this, chunkConfig);
        } else {
            log.error("unsupported chunk object info {}", chunkInfo);
            throw new CSException("unsupported chunk object");
        }
        return chunkObject;
    }

    private Long nextTag() {
        while (true) {
            var oldTag = minTag.get();
            var newTag = Math.max(System.currentTimeMillis(), oldTag + 1);
            if (minTag.compareAndSet(oldTag, newTag)) {
                return newTag;
            }
        }
    }

    @Override
    protected int dtServerPort() {
        return Cluster.cmPort;
    }
}
