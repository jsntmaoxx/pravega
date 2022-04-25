package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ReadRangeSLocation extends ReadRange {
    private final Location location;

    public ReadRangeSLocation(Location location, ChunkCache chunkCache) {
        super(chunkCache, 0);
        this.location = location;
    }

    public ReadRangeSLocation(Location location, ChunkObject chunkObj) {
        super(null, 0);
        this.location = location;
        this.activeChunkRange.compareAndSet(null, new ChunkSegmentRange(this, chunkObj, location));
    }

    @Override
    public String requestId() {
        return "r-" + requestId + "-s";
    }

    @Override
    public long contentStartOffset() {
        return 0;
    }

    @Override
    public long contentEndOffset() {
        return location.logicalEndOffset();
    }

    @Override
    public long contentLength() {
        return location.logicalLength();
    }

    @Override
    public RangeType rangeType() {
        return RangeType.Object;
    }

    @Override
    public int skipReadSize(int bufferSize) {
        return 0;
    }

    @Override
    public int skipReadSize() {
        return 0;
    }

    @Override
    public CompletableFuture<ReadDataBuffer> read(Executor executor) throws Exception {
        var f = new CompletableFuture<ReadDataBuffer>();
        var chunkRange = activeChunkRange.getOpaque();
        if (chunkRange != null) {
            chunkRange.chunkObject().read(chunkRange, executor, f);
            return f;
        }

        assert chunkCache != null;
        var chunkObj = chunkCache.get(location.chunkId);
        if (chunkObj != null) {
            activeChunkRange.compareAndSet(null, new ChunkSegmentRange(this, chunkObj, location));
            chunkObj.read(activeChunkRange.getOpaque(), executor, f);
            return f;
        }
        chunkCache.query(location.chunkId).thenApply(c -> {
            try {
                activeChunkRange.compareAndSet(null, new ChunkSegmentRange(this, c, location));
                c.read(activeChunkRange.getOpaque(), executor, f);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
            return null;
        });
        return f;
    }

    @Override
    public boolean valid() {
        return contentLength() < location.length;
    }

    @Override
    public String toString() {
        return new StringBuilder(64).append("seg ").append(location.chunkId)
                                    .append("[").append(location.offset).append(",")
                                    .append(location.endOffset()).append(")")
                                    .append(location.length)
                                    .append(" obj[").append(location.logicalOffset()).append(",")
                                    .append(location.logicalEndOffset()).append(")")
                                    .append(location.logicalLength())
                                    .toString();
    }
}
