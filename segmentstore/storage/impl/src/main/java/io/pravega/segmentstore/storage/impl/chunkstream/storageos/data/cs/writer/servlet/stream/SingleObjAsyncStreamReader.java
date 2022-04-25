package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj.ObjectWriter;
import jakarta.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleObjAsyncStreamReader extends AsyncStreamReader implements InputDataChunkFetcher {
    private static final Logger log = LoggerFactory.getLogger(SingleObjAsyncStreamReader.class);
    private static final Count onDataAvailableCount = Metrics.makeMetric("SingleObj.onDataAvailable.count", Count.class);
    protected int partId;
    private ObjectWriter objectWriter;

    public SingleObjAsyncStreamReader(AsyncContext asyncContext,
                                      String objKey,
                                      CSServletInputStream inputStream,
                                      long contentLength,
                                      int indexGranularity,
                                      CSConfiguration csConfig) {
        super(objKey, inputStream, contentLength, indexGranularity, asyncContext, csConfig);
    }

    public void setObjectWriter(ObjectWriter objectWriter) {
        this.objectWriter = objectWriter;
    }

    @Override
    public void onDataAvailable() {
        onDataAvailableCount.increase();
        read();
    }

    @Override
    public void onAllDataRead() {
        objectWriter.onFinishReadData(asyncContext);
    }

    @Override
    public void onError(Throwable t) {
        objectWriter.onErrorReadData(asyncContext, t);
    }

    @Override
    protected boolean offerData(WriteDataBuffer buffer) throws CSException {
        return objectWriter.onData(buffer);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected WriteDataBuffer makeBuffer(long objStart) {
        if (leftLength > 0) {
            // cj_todo use buffer pool
            // directory buffer pool will better
            // use native mem pool will eliminate mem copy
            // use rdma registered native mem pool will eliminate all mem copy in write path
            var size = Math.min((int) leftLength, indexGranularity);
            if (size >= CSConfiguration.writeSegmentSize()) {
                assert size == CSConfiguration.writeSegmentSize();
                return csConfig.useNativeBuffer()
                       ? SingleWriteBatchNativeWriteDataBuffer.allocate(objStart, size, requestId, partId++)
                       : SingleWriteBatchHeapWriteDataBuffer.allocate(objStart, size, requestId, partId++);
            }
            return csConfig.useNativeBuffer()
                   ? NativeWriteDataBuffer.allocate(objStart, size, requestId, partId++)
                   : HeapWriteDataBuffer.allocate(objStart, size, requestId, partId++);
        }
        return StringUtils.dumbBuffer;
    }

    @Override
    protected boolean requireExtraFetchData() {
        return objectWriter.requireExtraFetchData();
    }

    @Override
    public void onFetchChunkData() {
        if (requireData.compareAndSet(false, true)) {
            read();
        }
    }

    @Override
    public void onWriteError(Throwable t) {
        objectWriter.onErrorReadData(asyncContext, t);
    }

}
