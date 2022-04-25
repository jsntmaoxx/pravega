package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.concurrent.CompletableFuture;

public class FutureHeapWriteDataBuffer extends HeapWriteDataBuffer {
    protected final CompletableFuture<Location> completeFuture;

    protected FutureHeapWriteDataBuffer(long logicalOffset, int capacity, long requestId, int partId) {
        super(logicalOffset, capacity, requestId, partId);
        this.completeFuture = new CompletableFuture<>();
    }

    protected FutureHeapWriteDataBuffer(long logicalOffset, byte[] buffer, int position, int limit, int capacity,
                                        CompletableFuture<Location> completeFuture,
                                        long requestId, int partId, long createNano) {
        super(logicalOffset, buffer, position, limit, capacity, requestId, partId, createNano);
        this.completeFuture = completeFuture;
    }

    public static FutureHeapWriteDataBuffer allocate(long logicalOffset, int capacity, long requestId, int partId) {
        return new FutureHeapWriteDataBuffer(logicalOffset, capacity, requestId, partId);
    }

    @Override
    public FutureHeapWriteDataBuffer duplicate() {
        return new FutureHeapWriteDataBuffer(logicalOffset, buffer, position, limit, capacity, completeFuture, requestId, partId, createNano);
    }

    @Override
    public FutureHeapWriteDataBuffer duplicateData() {
        return new FutureHeapWriteDataBuffer(logicalOffset,
                                             buffer,
                                             CSConfiguration.writeSegmentHeaderLen(),
                                             capacity - CSConfiguration.writeSegmentFooterLen(),
                                             capacity,
                                             completeFuture,
                                             requestId,
                                             partId,
                                             createNano);
    }

    @Override
    public boolean markComplete(Location location, ChunkObject chunkObj) {
        super.markComplete(location, chunkObj);
        return completeFuture.complete(location);
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
    public CompletableFuture<Location> completeFuture() {
        return completeFuture;
    }

    @Override
    public String toString() {
        return String.format("r%d-%d-fhb", requestId, partId);
    }
}
