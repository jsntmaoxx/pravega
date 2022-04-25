package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto.StreamPosition;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class StreamWriteDataBuffer implements WriteDataBuffer<StreamPosition> {
    private static final Duration repoCRCDuration = Metrics.makeMetric("Heap.buffer.repo.crc.duration", Duration.class);

    private final ByteBuffer[] vData = new ByteBuffer[3];
    private final long createNano;
    private final CompletableFuture<StreamPosition> completeFuture;
    private final AtomicLong streamLogicalOffset;
    private final AtomicLong streamPhysicalOffset;
    private long bufferLogicalOffset = -1;
    private final long requestId;

    public StreamWriteDataBuffer(AtomicLong streamLogicalOffset, AtomicLong streamPhysicalOffset, ByteBuffer data, long requestId) {
        this.streamLogicalOffset = streamLogicalOffset;
        this.streamPhysicalOffset = streamPhysicalOffset;
        this.requestId = requestId;
        this.vData[0] = ByteBuffer.allocate(CSConfiguration.writeSegmentHeaderLen());
        this.vData[1] = data;
        this.vData[2] = ByteBuffer.allocate(CSConfiguration.writeSegmentFooterLen());
        this.createNano = System.nanoTime();
        this.completeFuture = new CompletableFuture<>();
    }

    @Override
    public byte[] array() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get array");
    }

    @Override
    public StreamWriteDataBuffer advancePosition(int len) {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support advancePosition");
    }

    @Override
    public int position() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get position");
    }

    @Override
    public StreamWriteDataBuffer limit(int limit) {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support set limit");
    }

    @Override
    public int limit() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get limit");
    }

    @Override
    public StreamWriteDataBuffer truncateAtCurrentDataPosition() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support truncateAtCurrentDataPosition");
    }

    @Override
    public StreamWriteDataBuffer reset() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support reset");
    }

    @Override
    public StreamWriteDataBuffer resetData() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support resetData");
    }

    @Override
    public int size() {
        return vData[1].remaining() + CSConfiguration.writeSegmentOverhead();
    }

    @Override
    public boolean hasRemaining() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get hasRemaining");
    }

    @Override
    public StreamWriteDataBuffer duplicate() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support duplicate");
    }

    @Override
    public StreamWriteDataBuffer duplicateData() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support duplicateData");
    }

    @Override
    public StreamWriteDataBuffer updateHeader() {
        vData[0].putInt(vData[1].remaining());
        vData[0].clear();
        return this;
    }

    @Override
    public StreamWriteDataBuffer updateFooterChecksum() {
        var start = System.nanoTime();
        CRC32 crc32 = new CRC32();
        crc32.update(vData[0]);
        vData[0].clear();
        vData[1].mark();
        crc32.update(vData[1]);
        vData[1].reset();
        vData[2].putLong(crc32.getValue());
        vData[2].clear();
        repoCRCDuration.updateNanoSecond(System.nanoTime() - start);
        return this;
    }

    @Override
    public StreamWriteDataBuffer updateFooterEpoch(byte[] epoch) {
        vData[2].position(Long.BYTES);
        vData[2].put(epoch);
        vData[2].clear();
        return this;
    }

    @Override
    public boolean markComplete(Location location, ChunkObject chunkObj) {
        var dataLen = vData[1].remaining();
        bufferLogicalOffset = streamLogicalOffset.getAndAdd(dataLen);
        streamPhysicalOffset.addAndGet(dataLen + CSConfiguration.writeSegmentOverhead());
        location.logicalOffset(bufferLogicalOffset);
        location.logicalLength(dataLen);

        var streamObj = (StreamChunkObject) chunkObj;
        return completeFuture.complete(StreamPosition.newBuilder()
                                                     .setStreamSequence(streamObj.streamSequence())
                                                     .setStreamLogicalOffset(location.logicalOffset())
                                                     .setStreamPhysicalOffset(streamObj.streamStartPhysicalOffset() + location.offset)
                                                     .setChunkOrder(streamObj.chunkOrder())
                                                     .setChunkIdHigh(location.chunkId.getMostSignificantBits())
                                                     .setChunkIdLow(location.chunkId.getLeastSignificantBits())
                                                     .setChunkOffset(location.offset)
                                                     .build());
    }

    @Override
    public boolean markCompleteWithException(Throwable t) {
        return completeFuture.completeExceptionally(t);
    }

    @Override
    public boolean cancelFuture(boolean canInterrupt) {
        return completeFuture.cancel(canInterrupt);
    }

    @Override
    public CompletableFuture<StreamPosition> completeFuture() {
        return completeFuture;
    }

    @Override
    public boolean isBatchBuffer() {
        return false;
    }

    @Override
    public WriteBatchBuffers toBatchBuffer() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support toBatchBuffer");
    }

    @Override
    public long createNano() {
        return createNano;
    }

    @Override
    public long logicalOffset() {
        if (bufferLogicalOffset < 0) {
            throw new CSRuntimeException("StreamWriteDataBuffer have not set logical offset");
        }
        return bufferLogicalOffset;
    }

    @Override
    public long logicalLength() {
        return vData[1].remaining();
    }

    @Override
    public boolean isNative() {
        return vData[1].isDirect();
    }

    @Override
    public long address() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get address");
    }

    @Override
    public void release() {
    }

    @Override
    public ByteBuffer shallowByteBuffer() {
        throw new CSRuntimeException("StreamWriteDataBuffer is not support get shallowByteBuffer");
    }

    @Override
    public ByteBuf wrapToByteBuf() {
        return Unpooled.wrappedBuffer(vData);
    }

    @Override
    public String toString() {
        return String.format("r%d-sb", requestId);
    }
}
