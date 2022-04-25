package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SingleWriteBatchHeapWriteDataBuffer extends HeapWriteDataBuffer implements WriteBatchBuffers {
    private static final Logger log = LoggerFactory.getLogger(SingleWriteBatchHeapWriteDataBuffer.class);
    private static final Duration WriteDuration = Metrics.makeMetric("SingleBatchHeapDataBuffer.write.duration", Duration.class);

    protected SingleWriteBatchHeapWriteDataBuffer(long logicalOffset, int dataCapacity, long requestId, int partId) {
        super(logicalOffset, dataCapacity, requestId, partId);
    }

    public SingleWriteBatchHeapWriteDataBuffer(long logicalOffset, byte[] buffer, int position, int limit, int capacity, long requestId, int partId, long createNano) {
        super(logicalOffset, buffer, position, limit, capacity, requestId, partId, createNano);
    }

    public static SingleWriteBatchHeapWriteDataBuffer allocate(long logicalOffset, int dataCapacity, long requestId, int partId) {
        return new SingleWriteBatchHeapWriteDataBuffer(logicalOffset, dataCapacity, requestId, partId);
    }

    @Override
    public SingleWriteBatchHeapWriteDataBuffer duplicate() {
        return new SingleWriteBatchHeapWriteDataBuffer(logicalOffset, buffer, position, limit, capacity, requestId, partId, createNano);
    }

    @Override
    public SingleWriteBatchHeapWriteDataBuffer duplicateData() {
        return new SingleWriteBatchHeapWriteDataBuffer(logicalOffset,
                                                       buffer,
                                                       CSConfiguration.writeSegmentHeaderLen(),
                                                       capacity - CSConfiguration.writeSegmentFooterLen(),
                                                       capacity,
                                                       requestId,
                                                       partId,
                                                       createNano);
    }

    @Override
    public boolean add(WriteDataBuffer buffer) {
        throw new UnsupportedOperationException("Do not add buffer to SingleBatchSizeHeapBuffer");
    }

    @Override
    public List<WriteDataBuffer> buffers() {
        return Collections.singletonList(this);
    }

    @Override
    public void completeWithException(Throwable t) {
        markCompleteWithException(t);
    }

    @Override
    public void complete(Location location, ChunkObject chunkObj) {
        WriteDuration.updateNanoSecond(System.nanoTime() - createNano);
        markComplete(location, chunkObj);
    }

    @Override
    public String requestId() {
        return "r" + requestId + "-" + partId;
    }

    @Override
    public void seal() {
    }

    @Override
    public WriteDataBuffer getBuffer(int i) {
        return this;
    }

    @Override
    public String toString() {
        return String.format("r%d-%d-b-hb", requestId, partId);
    }

    @Override
    public boolean isBatchBuffer() {
        return true;
    }

    @Override
    public WriteBatchBuffers toBatchBuffer() {
        return this;
    }

    @Override
    public long firstDataBufferCreateNano() {
        return createNano;
    }

    @Override
    public WriteBatchBuffers[] split(int splitPos) throws CSException {
        if (splitPos >= size()) {
            log.error("split buffer {} at splitPos {} failed", this, splitPos);
            throw new CSException("split splitPos less than buffer size");
        }
        return new WriteBatchBuffers[]{
                new SplitSingleWriteBatchBuffers(this, 0, duplicate().limit(position() + splitPos)),
                new SplitSingleWriteBatchBuffers(this, 1, duplicate().advancePosition(splitPos))
        };
    }

    @Override
    public void updateEpoch(byte[] epoch) {
        updateFooterEpoch(epoch);
    }
}
