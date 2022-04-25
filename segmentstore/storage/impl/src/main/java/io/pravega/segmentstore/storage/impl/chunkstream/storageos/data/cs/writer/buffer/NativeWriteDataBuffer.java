package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.NativeMemory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class NativeWriteDataBuffer extends BaseWriteDataBuffer {
    public static final Count memAllocate = Metrics.makeMetric("Native.mem.allocate.count", Count.class);
    public static final Count memFree = Metrics.makeMetric("Native.mem.free.count", Count.class);
    public static final Duration repoCRCDuration = Metrics.makeMetric("Native.buffer.repo.crc.duration", Duration.class);
    public static final Duration memAllocateDuration = Metrics.makeMetric("Native.mem.allocate.duration", Duration.class);
    public static final Duration numaMemAllocateDuration = Metrics.makeMetric("Native.numa.mem.allocate.duration", Duration.class);
    protected final long bufferAddress;
    protected final AtomicInteger refCount;

    protected NativeWriteDataBuffer(long logicalOffset, int dataCapacity, long requestId, int partId) {
        super(logicalOffset, dataCapacity, requestId, partId);
        var start = System.nanoTime();
        if (CSConfiguration.nativeBufferNuma() >= 0) {
            this.bufferAddress = NativeMemory.numa_allocate(this.capacity, CSConfiguration.nativeBufferNuma());
            numaMemAllocateDuration.updateNanoSecond(System.nanoTime() - start);
        } else {
            this.bufferAddress = NativeMemory.allocate(this.capacity);
            memAllocateDuration.updateNanoSecond(System.nanoTime() - start);
        }
        memAllocate.increase();
        this.refCount = new AtomicInteger(1);
    }

    protected NativeWriteDataBuffer(long logicalOffset, long address, int position, int limit, int capacity, long requestId, int partId, long createNano, AtomicInteger refCount) {
        super(logicalOffset, position, limit, capacity, requestId, partId, createNano);
        this.bufferAddress = address;
        refCount.incrementAndGet();
        this.refCount = refCount;
    }

    public static NativeWriteDataBuffer allocate(long logicalOffset, int dataCapacity, long requestId, int partId) {
        return new NativeWriteDataBuffer(logicalOffset, dataCapacity, requestId, partId);
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("Can not access buffer array on HeapWriteDataBuffer");
    }

    @Override
    public NativeWriteDataBuffer duplicate() {
        return new NativeWriteDataBuffer(logicalOffset, bufferAddress, position, limit, capacity, requestId, partId, createNano, refCount);
    }

    @Override
    public NativeWriteDataBuffer duplicateData() {
        return new NativeWriteDataBuffer(logicalOffset,
                                         bufferAddress,
                                         CSConfiguration.writeSegmentHeaderLen(),
                                         capacity - CSConfiguration.writeSegmentFooterLen(),
                                         capacity,
                                         requestId,
                                         partId,
                                         createNano,
                                         refCount);
    }

    @Override
    public NativeWriteDataBuffer updateHeader() {
        G.U.putInt(bufferAddress, Integer.reverseBytes(capacity - CSConfiguration.writeSegmentOverhead()));
        return this;
    }

    @Override
    public NativeWriteDataBuffer updateFooterChecksum() {
        if (CSConfiguration.skipRepoCRC) {
            return this;
        }
        var start = System.nanoTime();
        var checksum = CRC.crc32(0, bufferAddress, capacity - CSConfiguration.writeSegmentFooterLen());
        G.U.putLong(bufferAddress + capacity - CSConfiguration.writeSegmentFooterLen(), Long.reverseBytes(checksum));
        repoCRCDuration.updateNanoSecond(System.nanoTime() - start);
        return this;
    }

    @Override
    public NativeWriteDataBuffer updateFooterEpoch(byte[] epoch) {
        G.U.copyMemory(epoch, ARRAY_BASE_OFFSET, null, bufferAddress + capacity - CSConfiguration.writeSegmentFooterEpochLen(), epoch.length);
        return this;
    }

    @Override
    public boolean isBatchBuffer() {
        return false;
    }

    @Override
    public WriteBatchBuffers toBatchBuffer() {
        throw new ClassCastException("NativeWriteDataBuffer is not BatchBuffer");
    }

    @Override
    public boolean markComplete(Location location, ChunkObject chunkObj) {
        release();
        return super.markComplete(location, chunkObj);
    }

    @Override
    public boolean markCompleteWithException(Throwable t) {
        release();
        return super.markCompleteWithException(t);
    }

    @Override
    public boolean cancelFuture(boolean canInterrupt) {
        release();
        return super.cancelFuture(canInterrupt);
    }

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public long address() {
        return bufferAddress;
    }

    @Override
    public void release() {
        if (refCount.decrementAndGet() == 0) {
            if (CSConfiguration.nativeBufferNuma() >= 0) {
                NativeMemory.numa_free(bufferAddress, capacity);
            } else {
                NativeMemory.free(bufferAddress);
            }
            memFree.increase();
        }
    }

    @Override
    public ByteBuffer shallowByteBuffer() {
        return NativeMemory.directBuffer(bufferAddress + position, size());
    }

    @Override
    public ByteBuf wrapToByteBuf() {
        return Unpooled.wrappedBuffer(NativeMemory.directBuffer(bufferAddress + position, size()));
    }

    @Override
    public String toString() {
        return String.format("r%d-%d-nb", requestId, partId);
    }
}
