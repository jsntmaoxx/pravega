package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkPrefetch {
    private static final Logger log = LoggerFactory.getLogger(ChunkPrefetch.class);
    private static final Count fetchType1ChunkPrefetchCount = Metrics.makeMetric("ChunkI.fetch.prefetch.count", Count.class);
    private static final Duration fetchType1ChunkCreateDuration = Metrics.makeMetric("ChunkI.fetch.create.duration", Duration.class);
    private static final Count fetchType2ChunkPrefetchCount = Metrics.makeMetric("ChunkII.fetch.prefetch.count", Count.class);
    private static final Count fetchNormalChunkPrefetchCount = Metrics.makeMetric("ChunkNormal.fetch.prefetch.count", Count.class);
    private final CmClient cmClient;
    private final int bucketIndex;
    private final ExecutorService executor;
    private final CSConfiguration csConfig;
    private final BlockingQueue<ChunkObject> type1ChunkObjects = new BlockingArrayQueue<>();
    private final Queue<CompletableFuture<ChunkObject>> type1ChunkCreateFutures = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<ChunkObject> type2ChunkObjects = new BlockingArrayQueue<>();
    private final Queue<CompletableFuture<ChunkObject>> type2ChunkCreateFutures = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<ChunkObject> normalChunkObjects = new BlockingArrayQueue<>();
    private final Queue<CompletableFuture<ChunkObject>> normalChunkCreateFutures = new ConcurrentLinkedQueue<>();
    private final AtomicInteger creatingChunkTasks = new AtomicInteger(0);
    private int MinCachedChunkNum = 10;
    private int MaxCachedChunkNum = 50;
    private int MaxCreatingChunkTasks = 30;

    public ChunkPrefetch(CmClient cmClient, int bucketIndex, ExecutorService executor, CSConfiguration csConfig) {
        this.cmClient = cmClient;
        this.bucketIndex = bucketIndex;
        this.executor = executor;
        this.csConfig = csConfig;
    }

    public void prefetch() {
        triggerCreateChunk(type1ChunkObjects, type1ChunkCreateFutures, MinCachedChunkNum + 1, ChunkType.I);
        triggerCreateChunk(type2ChunkObjects, type2ChunkCreateFutures, MinCachedChunkNum + 1, ChunkType.II);
        triggerCreateChunk(normalChunkObjects, normalChunkCreateFutures, MinCachedChunkNum + 1, ChunkType.Normal);
    }

    public void prefetchType2() {
        triggerCreateChunk(type2ChunkObjects, type2ChunkCreateFutures, MaxCachedChunkNum, ChunkType.II);
    }

    public void setMinCachedChunkNum(int minCachedChunkNum) {
        MinCachedChunkNum = minCachedChunkNum;
    }

    public void setMaxCachedChunkNum(int maxCachedChunkNum) {
        MaxCachedChunkNum = maxCachedChunkNum;
    }

    public void setMaxCreatingChunkTasks(int maxCreatingChunkTasks) {
        MaxCreatingChunkTasks = maxCreatingChunkTasks;
    }

    public CompletableFuture<ChunkObject> fetchType1Chunk() {
        return fetchChunk(type1ChunkObjects, type1ChunkCreateFutures, ChunkType.I, fetchType1ChunkPrefetchCount);
    }

    public CompletableFuture<ChunkObject> fetchType2Chunk() {
        return fetchChunk(type2ChunkObjects, type2ChunkCreateFutures, ChunkType.II, fetchType2ChunkPrefetchCount);
    }

    public CompletableFuture<ChunkObject> fetchNormalChunk() {
        return fetchChunk(normalChunkObjects, normalChunkCreateFutures, ChunkType.Normal, fetchNormalChunkPrefetchCount);
    }

    public CompletableFuture<ChunkObject> createType2Chunk(UUID chunkUUID, int indexGranularity, ChunkConfig chunkConfig, String objName) throws CSException, IOException {
        return cmClient.createChunk(ChunkType.II, chunkUUID, indexGranularity, chunkConfig, objName);
    }


    public CompletableFuture<ChunkObject> createNormalChunk(UUID chunkUUID, int indexGranularity) throws CSException, IOException {
        return cmClient.createChunk(ChunkType.Normal, chunkUUID, indexGranularity);
    }

    private CompletableFuture<ChunkObject> fetchChunk(BlockingQueue<ChunkObject> chunkObjects,
                                                      Queue<CompletableFuture<ChunkObject>> chunkCreateFutures,
                                                      ChunkType chunkType,
                                                      Count prefetchCount) {
        var createFuture = new CompletableFuture<ChunkObject>();
        var c = chunkObjects.poll();
        if (c != null) {
            createFuture.complete(c);
            prefetchCount.increase();
        } else {
            chunkCreateFutures.add(createFuture);
        }
        var cacheSize = chunkObjects.size();
        if (cacheSize < MinCachedChunkNum && creatingChunkTasks.getOpaque() < MaxCachedChunkNum) {
            try {
                triggerCreateChunk(chunkObjects, chunkCreateFutures, MaxCachedChunkNum - cacheSize, chunkType);
            } catch (Exception e) {
                log.error("Trigger create chunk failed, cached {} creating tasks {}", chunkObjects.size(), creatingChunkTasks.getOpaque(), e);
            }
        }
        return createFuture;
    }

    private void triggerCreateChunk(BlockingQueue<ChunkObject> chunkObjects,
                                    Queue<CompletableFuture<ChunkObject>> chunkCreateFutures,
                                    int count,
                                    ChunkType chunkType) {
        for (var i = 0; i < count; ++i) {
            if (creatingChunkTasks.get() > MaxCreatingChunkTasks) {
                break;
            }
            try {
                creatingChunkTasks.incrementAndGet();
                executor.execute(() -> {
                    try {
                        cmClient.createChunk(chunkType, CmClient.generateNoUniqueChunkObjectId(bucketIndex), CSConfiguration.defaultIndexGranularity())
                                .whenComplete((c, t) -> {
                                    try {
                                        if (t != null) {
                                            log.error("create chunk type {} failed", chunkType, t);
                                        } else {
                                            var createFuture = chunkCreateFutures.poll();
                                            if (createFuture != null) {
                                                createFuture.complete(c);
                                            } else if (!chunkObjects.offer(c)) {
                                                // should not happen
                                                log.error("WSCritical: offer chunk {} to cache failed", c.chunkIdStr());
                                                c.triggerSeal(0);
                                            }
                                        }
                                    } finally {
                                        if (creatingChunkTasks.decrementAndGet() == 0 && chunkObjects.isEmpty()) {
                                            triggerCreateChunk(chunkObjects, chunkCreateFutures, MaxCachedChunkNum - chunkObjects.size(), chunkType);
                                        }
                                    }
                                });
                    } catch (Exception e) {
                        log.error("create chunk type {} failed", chunkType, e);
                    }
                });
            } catch (RejectedExecutionException ex) {
                creatingChunkTasks.decrementAndGet();
                if (!executor.isShutdown()) {
                    log.error("create chunk task was rejected, cached {} creating tasks {}", chunkObjects.size(), creatingChunkTasks.getOpaque(), ex);
                }
            }
        }
    }

    public void shutdown() {
        if (!executor.isShutdown()) {
            executor.shutdown();
        }
    }

    public boolean remove(UUID chunkId) {
        var success = type2ChunkObjects.remove(chunkId);
        if (success) {
            log.info("remove chunk {} from type II prefetch", chunkId);
        } else {
            success = type1ChunkObjects.remove(chunkId);
            if (success) {
                log.info("remove chunk {} from type I prefetch", chunkId);
            }
        }
        return success;
    }


    ChunkObject peekType2Chunk() {
        return type2ChunkObjects.isEmpty() ? null : type2ChunkObjects.peek();
    }

    public void clear() {
        log.info("clear chunk prefetch type II {} type I {}", type2ChunkObjects.size(), type1ChunkObjects.size());
        type2ChunkObjects.clear();
        type2ChunkCreateFutures.clear();
        type1ChunkObjects.clear();
        type1ChunkCreateFutures.clear();
        normalChunkObjects.clear();
        normalChunkCreateFutures.clear();
    }
}
