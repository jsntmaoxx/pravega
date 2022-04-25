package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.LocationCoder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj.SharedObjWriter;
import jakarta.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SharedSmallObjAsyncStreamReader extends SharedObjAsyncStreamReader {
    private static final Logger log = LoggerFactory.getLogger(SharedSmallObjAsyncStreamReader.class);
    private static final Duration SharedSmallObjCreateDuration = Metrics.makeMetric("SharedSmallObj.create.duration", Duration.class);
    private static final Duration SharedSmallObjReadStreamDataDuration = Metrics.makeMetric("SharedSmallObj.readStreamData.duration", Duration.class);
    private CompletableFuture<Location> writeFuture;

    public SharedSmallObjAsyncStreamReader(SharedObjWriter sharedObjWriter,
                                           AsyncContext asyncContext,
                                           String objKey,
                                           CSServletInputStream inputStream,
                                           long contentLength,
                                           int indexGranularity,
                                           LocationCoder.CodeType locationType,
                                           CSConfiguration csConfig,
                                           long startNs) {
        super(objKey, inputStream, contentLength, indexGranularity, asyncContext, sharedObjWriter, locationType, csConfig, startNs);
    }

    @Override
    protected boolean offerData(WriteDataBuffer buffer) throws CSException, InterruptedException {
        SharedSmallObjReadStreamDataDuration.updateNanoSecond(System.nanoTime() - startNano);
        assert writeFuture == null;
        log.debug("obj {} offer {}", objKey, buffer);
        sharedObjWriter.onData(buffer);
        writeFuture = buffer.completeFuture();
        eTagHandler.offer(buffer.duplicateData());
        // return true to finish read
        return true;
    }

    @Override
    public void onFetchObjData() {
        log().warn("unexpected call SharedSmallObjAsyncStreamReader.onFetchObjData()");
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected List<Location> getWriteLocations() throws ExecutionException, InterruptedException {
        assert eTagHandler.eTagFuture().isDone();
        return Collections.singletonList(writeFuture.get());
    }

    protected CompletableFuture<?>[] makeWaitFutures() {
        return new CompletableFuture[]{eTagHandler.eTagFuture(), writeFuture};
    }

    @Override
    protected int nextPartId() {
        return 0;
    }

    @Override
    protected void onSuccess() {
        SharedSmallObjCreateDuration.updateNanoSecond(System.nanoTime() - startNano);
    }
}
