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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SharedLargeObjAsyncStreamReader extends SharedObjAsyncStreamReader {
    private static final Logger log = LoggerFactory.getLogger(SharedLargeObjAsyncStreamReader.class);
    private static final Duration SharedLargeObjCreateDuration = Metrics.makeMetric("SharedLargeObj.create.duration", Duration.class);

    private final List<CompletableFuture<Location>> writeFutures;
    protected int partId;

    public SharedLargeObjAsyncStreamReader(SharedObjWriter sharedObjWriter,
                                           AsyncContext asyncContext,
                                           String objKey,
                                           CSServletInputStream inputStream,
                                           long contentLength,
                                           int indexGranularity,
                                           LocationCoder.CodeType locationType,
                                           CSConfiguration csConfig,
                                           long startNs) {
        super(objKey, inputStream, contentLength, indexGranularity, asyncContext, sharedObjWriter, locationType, csConfig, startNs);
        var estimateNum = contentLength % this.indexGranularity != 0
                          ? contentLength / this.indexGranularity + 1
                          : contentLength / this.indexGranularity;
        this.writeFutures = new ArrayList<>((int) estimateNum);
        this.sharedObjWriter.registerObjDataFetcher(this);
    }

    /*
    offerData used by read(), which is protected by a mem fence cas
     */
    @Override
    protected boolean offerData(WriteDataBuffer buffer) throws CSException {
        log.debug("obj {} offer {}", objKey, buffer);
        var requireData = sharedObjWriter.onData(buffer);
        writeFutures.add(buffer.completeFuture());
        eTagHandler.offer(buffer.duplicateData());
        return requireData;
    }

    @Override
    public void onAllDataRead() throws IOException {
        this.sharedObjWriter.unregisterObjDataFetcher(this, "read all data");
        super.onAllDataRead();
    }

    @Override
    public void onError(Throwable t) {
        this.sharedObjWriter.unregisterObjDataFetcher(this, "on error");
        super.onError(t);
    }

    @Override
    public void onFetchObjData() {
        // shared object writer could trigger onFetchObjData before this obj stream reader finished.
        if (inputStream.isFinished()) {
            this.sharedObjWriter.unregisterObjDataFetcher(this, "input stream finished");
            return;
        }
        super.onFetchObjData();
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected List<Location> getWriteLocations() throws ExecutionException, InterruptedException {
        assert eTagHandler.eTagFuture().isDone();
        List<Location> locations = new ArrayList<>(writeFutures.size());
        for (var wf : writeFutures) {
            locations.add(wf.get());
        }
        return locations;
    }

    @Override
    protected CompletableFuture<?>[] makeWaitFutures() {
        var eTagFuture = eTagHandler.eTagFuture();
        List<CompletableFuture<?>> wfs = new ArrayList<>(2);
        wfs.add(eTagFuture);
        for (var cf : writeFutures) {
            if (!cf.isDone() || cf.isCompletedExceptionally()) {
                wfs.add(cf);
            }
        }
        CompletableFuture<?>[] cfs = new CompletableFuture<?>[wfs.size()];
        wfs.toArray(cfs);
        return cfs;
    }

    @Override
    protected int nextPartId() {
        return partId++;
    }

    @Override
    protected void onSuccess() {
        SharedLargeObjCreateDuration.updateNanoSecond(System.nanoTime() - startNano);
    }

}
