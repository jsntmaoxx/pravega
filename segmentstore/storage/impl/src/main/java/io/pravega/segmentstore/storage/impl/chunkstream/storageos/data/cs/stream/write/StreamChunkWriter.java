package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteNormalChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StreamChunkWriter implements StreamWriter, InputDataChunkFetcher {
    private static final Logger log = LoggerFactory.getLogger(StreamChunkWriter.class);

    private final String streamId;
    private final AtomicLong acceptStreamLogicalOffset = new AtomicLong(0);
    private final AtomicLong acceptStreamPhysicalOffset = new AtomicLong(0);
    private final CmClient cmClient;
    private final ChunkConfig chunkConfig;
    private final Executor executor;
    private volatile WriteChunkHandler writeChunkHandler;
    private CompletableFuture<Void> createChunkFuture;
    private final List<StreamChunkObject> chunkObjects = new ArrayList<>(2);

    /*
    use under lock of this
     */
    private enum DataFlowDirection {
        ToChunk,
        ToCache,
    }

    private final StreamDataCacheQueue dataCacheQueue;
    private DataFlowDirection dataFlowDirection = DataFlowDirection.ToChunk;

    public StreamChunkWriter(String streamId,
                             AtomicLong streamLogicalOffset,
                             AtomicLong streamPhysicalOffset,
                             CmClient cmClient,
                             ChunkConfig chunkConfig,
                             Executor executor) {
        this.streamId = streamId;
        this.acceptStreamLogicalOffset.set(streamLogicalOffset.get());
        this.acceptStreamPhysicalOffset.set(streamPhysicalOffset.get());
        this.cmClient = cmClient;
        this.chunkConfig = chunkConfig;
        this.executor = executor;
        this.dataCacheQueue = new StreamDataCacheQueue(streamId, CSConfiguration.streamDataCacheCapacity);
    }

    @Override
    public void offer(StreamWriteDataBuffer data) throws CSException {
        if (writeChunkHandler == null) {
            cacheAndRotateChunk(data);
            return;
        }
        if (!writeChunkHandler.acceptData()) {
            cacheAndRotateChunk(data);
            return;
        }
        if (writeChunkHandler.offer(data)) {
            var size = data.size();
            acceptStreamLogicalOffset.addAndGet(size - CSConfiguration.writeSegmentOverhead());
            acceptStreamPhysicalOffset.addAndGet(size);
            writeChunkHandler.writeNext();
            return;
        }
        cacheAndRotateChunk(data);
    }

    /*
    all rotation operation is this lock
     */
    private synchronized void cacheAndRotateChunk(StreamWriteDataBuffer data) throws CSException {
        if (dataFlowDirection == DataFlowDirection.ToCache) {
            dataCacheQueue.push(data);
            return;
        }

        if (writeChunkHandler == null) {
            dataFlowDirection = DataFlowDirection.ToCache;
            dataCacheQueue.push(data);
            rotateChunkAndPopCache("NIL");
            return;
        }

        if (writeChunkHandler.acceptData()) {
            if (writeChunkHandler.offer(data)) {
                var size = data.size();
                acceptStreamLogicalOffset.addAndGet(size - CSConfiguration.writeSegmentOverhead());
                acceptStreamPhysicalOffset.addAndGet(size);
                writeChunkHandler.writeNext();
                return;
            }
        }

        dataFlowDirection = DataFlowDirection.ToCache;
        dataCacheQueue.push(data);
        var preWriteHandler = writeChunkHandler;
        preWriteHandler.end();
        preWriteHandler.writeChunkFuture().whenComplete((r, t) -> {
            if (t != null) {
                log.error("stream {} write chunk {} encounter error", streamId, preWriteHandler.chunkIdStr(), t);
                //cj_todo add previous write batch to next chunk
                log.error("WSCritical: move unfinished write data from chunk {} to cache", writeChunkHandler.chunkIdStr());
            }
            ((StreamChunkObject) preWriteHandler.chunkObj()).updateStreamEndOffset(acceptStreamLogicalOffset.get(),
                                                                                   acceptStreamPhysicalOffset.get());
            rotateChunkAndPopCache(preWriteHandler.chunkIdStr());
        });
    }

    /*
    all rotation operation is this lock
     */
    private synchronized void rotateChunkAndPopCache(String preChunkId) {
        var chunkObject = nextCachedChunkObject();

        if (chunkObject == null) {
            triggerCreateChunk(CSConfiguration.streamPrefetchChunkCount).whenComplete((v, t) -> {
                if (t != null) {
                    log.error("stream {} failed to create chunk", streamId, t);
                    return;
                }
                rotateChunkAndPopCache(preChunkId);
            });
            return;
        }

        var writeHandler = rotateWriteChunkHandler(preChunkId, chunkObject);
        var data = dataCacheQueue.peak();
        while (data != null) {
            try {
                if (!writeHandler.offer(data)) {
                    break;
                }
                dataCacheQueue.pop();
                var size = data.size();
                acceptStreamLogicalOffset.addAndGet(size - CSConfiguration.writeSegmentOverhead());
                acceptStreamPhysicalOffset.addAndGet(size);
                writeHandler.writeNext();
                data = dataCacheQueue.peak();
            } catch (Exception e) {
                log.error("stream {} offer data {} failed", streamId, data, e);
                break;
            }
        }
        if (dataCacheQueue.peak() != null) {
            log.info("stream {} wait chunk {} finish write during rotate and pop cache", streamId, writeHandler.chunkIdStr());
            writeHandler.end();
            writeHandler.writeChunkFuture().whenComplete((r, t) -> {
                if (t != null) {
                    log.error("stream {} write chunk {} encounter error", streamId, writeHandler.chunkIdStr(), t);
                    //cj_todo add previous write batch to next chunk
                    log.error("WSCritical: move unfinished write data from chunk {} to cache", writeChunkHandler.chunkIdStr());
                }
                ((StreamChunkObject) writeHandler.chunkObj()).updateStreamEndOffset(acceptStreamLogicalOffset.get(),
                                                                                    acceptStreamPhysicalOffset.get());
                rotateChunkAndPopCache(writeHandler.chunkIdStr());
            });
            return;
        }
        // publish write chunk handler
        log.info("stream {} change data flow cache->chunk {}", streamId, writeHandler.chunkIdStr());
        dataFlowDirection = DataFlowDirection.ToChunk;
        writeChunkHandler = writeHandler;
    }

    private WriteNormalChunkHandler rotateWriteChunkHandler(String preChunkId, StreamChunkObject chunkObject) {
        log.info("stream {} rotate chunk {}->{} stream offset logical {} physical {}",
                 streamId, preChunkId, chunkObject.chunkIdStr(),
                 acceptStreamLogicalOffset.get(), acceptStreamPhysicalOffset.get());
        chunkObject.updateStreamStartOffset(acceptStreamLogicalOffset.get(), acceptStreamPhysicalOffset.get());
        return new WriteNormalChunkHandler(chunkObject, this, executor, chunkConfig.csConfig);
    }

    /*
    locked by caller
     */
    private CompletableFuture<Void> triggerCreateChunk(int count) {
        if (createChunkFuture != null) {
            if (!createChunkFuture.isDone()) {
                return createChunkFuture;
            }
            if (chunkObjects.size() >= CSConfiguration.streamPrefetchChunkCount) {
                return createChunkFuture;
            }
        }
        var f = new CompletableFuture<Void>();
        createChunkFuture = f;
        try {
            var me = this;
            cmClient.createStreamChunk(streamId, CmClient.generateChunkId(count), chunkConfig)
                    .whenComplete((r, t) -> {
                        if (t != null || r.isEmpty()) {
                            log.error("stream {} require create {} chunk failed", streamId, count);
                            f.completeExceptionally(t);
                            return;
                        }
                        if (r.size() == 1) {
                            log.info("stream {} require {} create {} chunk {}",
                                     streamId, count, r.size(), r.get(0).chunkIdStr());
                        } else {
                            log.info("stream {} require {} create {} chunk {}",
                                     streamId, count, r.size(),
                                     r.stream().map(ChunkObject::chunkIdStr).collect(Collectors.joining(", ")));
                        }
                        synchronized (me) {
                            chunkObjects.addAll(r);
                        }
                        f.complete(null);
                    });
        } catch (Exception e) {
            log.error("stream {} trigger create {} chunk failed", streamId, count, e);
            f.completeExceptionally(e);
        }
        return f;
    }

    /*
    locked by caller
     */
    private StreamChunkObject nextCachedChunkObject() {
        if (chunkObjects.isEmpty()) {
            triggerCreateChunk(CSConfiguration.streamPrefetchChunkCount + 1);
            return null;
        }
        var c = chunkObjects.remove(0);
        triggerCreateChunk(CSConfiguration.streamPrefetchChunkCount - chunkObjects.size());
        return c;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (writeChunkHandler != null) {
                writeChunkHandler.end();
            }
            for (var o : chunkObjects) {
                o.triggerSeal();
            }
        }
    }

    /*
    Implement InputDataChunkFetcher
     */
    @Override
    public void onFetchChunkData() {
    }

    @Override
    public void onWriteError(Throwable t) {
    }
}
