package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class HeapWriteDataBuffer extends BaseWriteDataBuffer {
    private static final Duration repoCRCDuration = Metrics.makeMetric("Heap.buffer.repo.crc.duration", Duration.class);

    protected final byte[] buffer;

    protected HeapWriteDataBuffer(long logicalOffset, int dataCapacity, long requestId, int partId) {
        super(logicalOffset, dataCapacity, requestId, partId);
        this.buffer = new byte[this.capacity];
    }

    protected HeapWriteDataBuffer(long logicalOffset, byte[] buffer, int position, int limit, int capacity, long requestId, int partId, long createNano) {
        super(logicalOffset, position, limit, capacity, requestId, partId, createNano);
        this.buffer = buffer;
    }

    public static HeapWriteDataBuffer allocate(long logicalOffset, int dataCapacity, long requestId, int partId) {
        return new HeapWriteDataBuffer(logicalOffset, dataCapacity, requestId, partId);
    }

    @Override
    public byte[] array() {
        return buffer;
    }

    @Override
    public HeapWriteDataBuffer duplicate() {
        return new HeapWriteDataBuffer(logicalOffset, buffer, position, limit, capacity, requestId, partId, createNano);
    }

    @Override
    public HeapWriteDataBuffer duplicateData() {
        return new HeapWriteDataBuffer(logicalOffset,
                                       buffer,
                                       CSConfiguration.writeSegmentHeaderLen(),
                                       capacity - CSConfiguration.writeSegmentFooterLen(),
                                       capacity,
                                       requestId,
                                       partId,
                                       createNano);
    }

    @Override
    public HeapWriteDataBuffer updateHeader() {
        G.U.putInt(buffer, ARRAY_BASE_OFFSET, Integer.reverseBytes(capacity - CSConfiguration.writeSegmentOverhead()));
        return this;
    }

    @Override
    public HeapWriteDataBuffer updateFooterChecksum() {
        if (CSConfiguration.skipRepoCRC) {
            return this;
        }
        var start = System.nanoTime();
        CRC32 crc32 = new CRC32();
        crc32.update(buffer, 0, capacity - CSConfiguration.writeSegmentFooterLen());
        G.U.putLong(buffer, ARRAY_BASE_OFFSET + capacity - CSConfiguration.writeSegmentFooterLen(), Long.reverseBytes(crc32.getValue()));
        repoCRCDuration.updateNanoSecond(System.nanoTime() - start);
        return this;
    }

    @Override
    public HeapWriteDataBuffer updateFooterEpoch(byte[] epoch) {
        assert epoch.length == CSConfiguration.writeSegmentFooterEpochLen();
        System.arraycopy(epoch, 0, buffer,
                         capacity - CSConfiguration.writeSegmentFooterEpochLen(),
                         CSConfiguration.writeSegmentFooterEpochLen());
        return this;
    }

    @Override
    public boolean isBatchBuffer() {
        return false;
    }

    @Override
    public WriteBatchBuffers toBatchBuffer() {
        throw new ClassCastException("HeapWriteDataBuffer is not BatchBuffer");
    }

    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public long address() {
        throw new UnsupportedOperationException("Can not access address on HeapWriteDataBuffer");
    }

    @Override
    public void release() {
    }

    @Override
    public ByteBuffer shallowByteBuffer() {
        return ByteBuffer.wrap(buffer, position, size());
    }

    @Override
    public ByteBuf wrapToByteBuf() {
        return Unpooled.wrappedBuffer(buffer, position, size());
    }

    @Override
    public String toString() {
        return String.format("r%d-%d-hb", requestId, partId);
    }
}
