package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkPrefetch;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.LocationCoder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class SObjMChunkWriter extends SingleObjWriter {
    private static final Logger log = LoggerFactory.getLogger(SObjMChunkWriter.class);
    private static final Duration SObjMChunkCreateDuration = Metrics.makeMetric("SObjSChunk.create.duration", Duration.class);
    protected final List<WriteChunkHandler> writeChunkHandlers = new ArrayList<>();
    protected final ChunkPrefetch chunkPrefetch;
    protected final Executor executor;
    protected WriteChunkHandler activeWriteChunkHandler;

    public SObjMChunkWriter(String objKey,
                            InputDataChunkFetcher inputDataFetcher,
                            ChunkObject chunkObj,
                            ChunkPrefetch chunkPrefetch,
                            Executor executor,
                            LocationCoder.CodeType locationType,
                            CSConfiguration csConfig,
                            long startNs,
                            long timeoutSeconds) {
        super(objKey, inputDataFetcher, locationType, csConfig, startNs, timeoutSeconds);
        this.chunkPrefetch = chunkPrefetch;
        this.executor = executor;
        this.activeWriteChunkHandler = createWriteChunkHandler(chunkObj, csConfig.defaultChunkConfig(), executor);
        this.writeChunkHandlers.add(this.activeWriteChunkHandler);
    }

    @Override
    public boolean onData(WriteDataBuffer buffer) throws CSException {
        var ret = activeWriteChunkHandler.offer(buffer);
        if (!ret) {
            int maxRetry = 3;
            do {
                if (maxRetry-- == 0) {
                    log.error("write obj {} failed to offer buffer {} byte after rotate {} chunks",
                              objKey, buffer.size(), maxRetry);
                    throw new CSException("failed to offer buffer to chunks after rotate " + maxRetry + " chunks");
                }
                var preChunk = activeWriteChunkHandler.chunkIdStr();
                log.info("write obj {} begin rotate pre chunk {} due to can't offer buffer to it", objKey, preChunk);
                rotateChunk();
                ret = activeWriteChunkHandler.offer(buffer);
                log.info("write obj {} rotate pre chunk {} to {}",
                         objKey, preChunk, activeWriteChunkHandler.chunkIdStr());
            } while (!ret);
        }

        // rotate chunk
        eTagHandler.offer(buffer.duplicateData());
        return activeWriteChunkHandler.writeNext();
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public boolean requireExtraFetchData() {
        return activeWriteChunkHandler.requireData();
    }

    @Override
    protected WriteChunkHandler activeWriteChunkHandler() {
        return activeWriteChunkHandler;
    }

    @Override
    public List<Location> getWriteLocations() {
        assert eTagHandler.eTagFuture().isDone();
        List<Location> locations = new ArrayList<>(writeChunkHandlers.size());
        for (var wh : writeChunkHandlers) {
            locations.add(wh.getWriteLocation());
        }
        return locations;
    }

    @Override
    protected CompletableFuture<?>[] makeWaitFutures() {
        var eTagFuture = eTagHandler.eTagFuture();
        List<CompletableFuture<?>> chunkFutures = new ArrayList<>(2);
        chunkFutures.add(eTagFuture);
        for (var ch : writeChunkHandlers) {
            var cf = ch.writeChunkFuture();
            if (!cf.isDone() || cf.isCompletedExceptionally()) {
                chunkFutures.add(cf);
            }
        }
        CompletableFuture<?>[] cfs = new CompletableFuture<?>[chunkFutures.size()];
        chunkFutures.toArray(cfs);
        return cfs;
    }

    @Override
    protected void errorEndWriteChunkHandlers() {
        for (var ch : writeChunkHandlers) {
            ch.cancel();
            ch.errorEnd();
        }
    }

    @Override
    protected void onSuccess() {
        SObjMChunkCreateDuration.updateNanoSecond(System.nanoTime() - startNano);
    }

    protected void rotateChunk() throws CSException {
        try {
            // cj_todo blocking operation,  async is complex, ref SteamChunkWriter
            var nc = chunkPrefetch.fetchType2Chunk().get();
            activeWriteChunkHandler.end();
            activeWriteChunkHandler = createWriteChunkHandler(nc, csConfig.defaultChunkConfig(), executor);
            writeChunkHandlers.add(activeWriteChunkHandler);
        } catch (Exception e) {
            log.error("write obj {} failed to rotate chunk, current chunk {}", objKey, activeWriteChunkHandler.chunkIdStr(), e);
            throw new CSException("failed to rotate chunk");
        }
    }
}
