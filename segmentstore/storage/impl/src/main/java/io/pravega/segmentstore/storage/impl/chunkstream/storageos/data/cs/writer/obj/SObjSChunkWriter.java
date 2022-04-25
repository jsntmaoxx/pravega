package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.LocationCoder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class SObjSChunkWriter extends SingleObjWriter {
    private static final Logger log = LoggerFactory.getLogger(SObjSChunkWriter.class);
    private static final Duration SObjSChunkCreateDuration = Metrics.makeMetric("SObjSChunk.create.duration", Duration.class);
    protected WriteChunkHandler writeChunkHandler;

    public SObjSChunkWriter(String objKey,
                            InputDataChunkFetcher inputDataFetcher,
                            ChunkObject chunkObj,
                            Executor executor,
                            LocationCoder.CodeType locationType,
                            ChunkConfig chunkConfig,
                            long startNs,
                            long timeoutSeconds) {
        super(objKey, inputDataFetcher, locationType, chunkConfig.csConfig, startNs, timeoutSeconds);
        this.writeChunkHandler = createWriteChunkHandler(chunkObj, chunkConfig, executor);
    }

    @Override
    public boolean onData(WriteDataBuffer buffer) throws CSException {
        var ret = writeChunkHandler.offer(buffer);
        if (!ret) {
            log.error("write obj {} failed to offer buffer {} size {} to chunk {}",
                      objKey, buffer, buffer.size(), writeChunkHandler.chunkIdStr());
            throw new CSException("failed to offer buffer to chunk " + writeChunkHandler.chunkIdStr());
        }

        eTagHandler.offer(buffer.duplicateData());
        return writeChunkHandler.writeNext();
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public boolean requireExtraFetchData() {
        return writeChunkHandler.requireData();
    }

    @Override
    protected WriteChunkHandler activeWriteChunkHandler() {
        return writeChunkHandler;
    }

    @Override
    public List<Location> getWriteLocations() {
        assert eTagHandler.eTagFuture().isDone();
        return Collections.singletonList(writeChunkHandler.getWriteLocation());
    }

    @Override
    protected CompletableFuture<?>[] makeWaitFutures() {
        return new CompletableFuture<?>[]{eTagHandler.eTagFuture(), writeChunkHandler.writeChunkFuture()};
    }

    @Override
    protected void errorEndWriteChunkHandlers() {
        writeChunkHandler.cancel();
        writeChunkHandler.errorEnd();
    }

    @Override
    protected void onSuccess() {
        SObjSChunkCreateDuration.updateNanoSecond(System.nanoTime() - startNano);
    }
}
