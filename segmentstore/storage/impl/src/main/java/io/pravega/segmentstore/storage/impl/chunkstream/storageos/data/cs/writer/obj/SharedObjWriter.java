package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkPrefetch;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteType1ChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataObjFetcher;
import jakarta.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class SharedObjWriter implements ObjectWriter, InputDataChunkFetcher {
    private static final Logger log = LoggerFactory.getLogger(SharedObjWriter.class);
    protected final ChunkPrefetch chunkPrefetch;
    private final Executor executor;
    private final CSConfiguration csConfig;
    private final ConcurrentSkipListSet<InputDataObjFetcher> objDataFetchers = new ConcurrentSkipListSet<>();
    private volatile WriteChunkHandler writeChunkHandler;

    public SharedObjWriter(ChunkPrefetch chunkPrefetch,
                           Executor executor,
                           CSConfiguration csConfig) {
        this.chunkPrefetch = chunkPrefetch;
        this.executor = executor;
        this.csConfig = csConfig;
    }

    public void registerObjDataFetcher(InputDataObjFetcher objFetcher) {
        objDataFetchers.add(objFetcher);
    }

    public boolean unregisterObjDataFetcher(InputDataObjFetcher objFetcher, String reason) {
        log().debug("unregister data fetcher {} reason {}", objFetcher.name(), reason);
        return objDataFetchers.remove(objFetcher);
    }

    /*
    Require thread safe
     */
    @Override
    public boolean onData(WriteDataBuffer buffer) throws CSException {
        if (writeChunkHandler == null) {
            synchronized (this) {
                if (writeChunkHandler == null) {
                    writeChunkHandler = createWriteChunkHandler(csConfig, this.executor);
                    log.info("create chunk {} for writing shared object {}",
                             writeChunkHandler.chunkIdStr(), buffer);
                    if (writeChunkHandler == null) {
                        buffer.markCompleteWithException(new CSException("can not create WriteChunkHandler"));
                        return false;
                    }
                }
            }
        }
        var ret = writeChunkHandler.offer(buffer);
        if (!ret) {
            int maxRetry = 3;
            do {
                if (maxRetry-- == 0) {
                    log.error("{} failed to offer buffer {} byte after rotate {} type1 chunk",
                              buffer, buffer.size(), maxRetry);
                    var ex = new CSException("failed to offer buffer to chunks after rotate " + maxRetry + " type1 chunk");
                    buffer.markCompleteWithException(ex);
                    throw ex;
                }
                var preChunk = writeChunkHandler.chunkIdStr();
                synchronized (this) {
                    if (preChunk.equals(writeChunkHandler.chunkIdStr())) {
                        log.info("begin rotate pre chunk {} due to can't offer {} buffer to it", preChunk, buffer);
                        rotateChunk();
                        log.info("rotate pre chunk {} to {}", preChunk, writeChunkHandler.chunkIdStr());
                    }
                }
                ret = writeChunkHandler.offer(buffer);
            } while (!ret);
        }

        return writeChunkHandler.writeNext();
    }

    @Override
    public void onFinishReadData(AsyncContext asyncContext) {
        throw new UnsupportedOperationException("Not allow SharedObjectWriter.onFinishReadData()");
    }

    @Override
    public void onErrorReadData(AsyncContext asyncContext, Throwable t) {
        throw new UnsupportedOperationException("Not allow SharedObjectWriter.onErrorReadData()");
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public boolean requireExtraFetchData() {
        return writeChunkHandler.requireData();
    }

    /*
    SingleObjectInputDataFetcher
     */
    @Override
    public void onFetchChunkData() {
        for (var f : objDataFetchers) {
            f.onFetchObjData();
        }
    }

    @Override
    public void onWriteError(Throwable t) {
        throw new UnsupportedOperationException("Not allow SharedObjectWriter.onWriteError()");
    }

    /*
    Not thread safe, use in lock
     */
    private void rotateChunk() throws CSException {
        try {
            writeChunkHandler.end();
            writeChunkHandler = createWriteChunkHandler(csConfig, executor);
        } catch (Exception e) {
            log.error("failed to rotate type1 chunk, current chunk {}", writeChunkHandler.chunkIdStr(), e);
            throw new CSException("failed to rotate chunk");
        }
    }

    /*
    Not thread safe, use in lock
     */
    protected WriteChunkHandler createWriteChunkHandler(CSConfiguration csConfig, Executor executor) throws CSException {
        ChunkObject chunkObj;
        try {
            // cj_todo blocking operation,  async is complex, ref SteamChunkWriter
            chunkObj = chunkPrefetch.fetchType1Chunk().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new CSException("fetch new type1 chunk failed", e);
        }
        if (chunkObj == null) {
            log().error("fetch new type1 chunk failed");
            return null;
        }
        var chunkConfig = csConfig.defaultChunkConfig();
        var ecSchema = chunkConfig.ecSchema();
        var codeMatrix = ecSchema.codeMatrixCache.borrow();
        var dataSegment = ecSchema.dataSegmentCache.borrow();
        if (codeMatrix != null && dataSegment != null) {
            return new WriteType1ChunkHandler(chunkObj, this, executor, codeMatrix, dataSegment, chunkConfig);
        }
        log().warn("chunk {} did not get code matrix and data segment buffer, change write type1 without client ec", chunkObj.chunkIdStr());
        ecSchema.codeMatrixCache.giveBack(codeMatrix);
        ecSchema.dataSegmentCache.giveBack(dataSegment);
        return new WriteType1ChunkHandler(chunkObj, this, executor, null, null, chunkConfig);
    }
}
