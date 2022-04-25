package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class BaseWriteDataBuffer implements WriteDataBuffer<Location> {
    protected static final CompletableFuture<Location> defaultCompleteFuture = new CompletableFuture<>();
    protected static final int ARRAY_BASE_OFFSET = G.U.arrayBaseOffset(byte[].class);
    private static final int ARRAY_INDEX_SCALE = G.U.arrayIndexScale(byte[].class);

    static {
        defaultCompleteFuture.complete(new Location(new UUID(0L, 0L), 0, 0));
    }

    protected final long logicalOffset;
    protected final long requestId;
    protected final int partId;
    protected final long createNano;
    protected int capacity;
    protected int position;
    protected int limit;


    protected BaseWriteDataBuffer(long logicalOffset, int dataCapacity, long requestId, int partId) {
        this.logicalOffset = logicalOffset;
        this.requestId = requestId;
        this.partId = partId;
        this.capacity = dataCapacity + CSConfiguration.writeSegmentOverhead();
        this.position = CSConfiguration.writeSegmentHeaderLen();
        this.limit = this.capacity - CSConfiguration.writeSegmentFooterLen();
        this.createNano = System.nanoTime();
    }

    protected BaseWriteDataBuffer(long logicalOffset, int position, int limit, int capacity, long requestId, int partId, long createNano) {
        this.logicalOffset = logicalOffset;
        this.capacity = capacity;
        this.position = position;
        this.limit = limit;
        this.requestId = requestId;
        this.partId = partId;
        this.createNano = createNano;
    }

    @Override
    public WriteDataBuffer advancePosition(int len) {
        assert len + position <= limit;
        this.position += len;
        return this;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public WriteDataBuffer limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public WriteDataBuffer truncateAtCurrentDataPosition() {
        assert position + CSConfiguration.writeSegmentFooterLen() <= capacity;
        capacity = position + CSConfiguration.writeSegmentFooterLen();
        return this;
    }

    @Override
    public WriteDataBuffer reset() {
        position = 0;
        limit = capacity;
        return this;
    }

    @Override
    public WriteDataBuffer resetData() {
        position = CSConfiguration.writeSegmentHeaderLen();
        limit = capacity - CSConfiguration.writeSegmentFooterLen();
        return this;
    }

    @Override
    public int size() {
        return limit - position;
    }

    @Override
    public boolean hasRemaining() {
        return limit > position;
    }

    @Override
    public boolean markComplete(Location location, ChunkObject chunkObj) {
        location.logicalOffset(logicalOffset());
        location.logicalLength(logicalLength());
        return true;
    }

    @Override
    public boolean markCompleteWithException(Throwable t) {
        return true;
    }

    @Override
    public boolean cancelFuture(boolean canInterrupt) {
        return false;
    }

    @Override
    public CompletableFuture<Location> completeFuture() {
        return defaultCompleteFuture;
    }

    @Override
    public long createNano() {
        return createNano;
    }

    @Override
    public long logicalOffset() {
        return logicalOffset;
    }

    @Override
    public long logicalLength() {
        return capacity - CSConfiguration.writeSegmentOverhead();
    }
}
