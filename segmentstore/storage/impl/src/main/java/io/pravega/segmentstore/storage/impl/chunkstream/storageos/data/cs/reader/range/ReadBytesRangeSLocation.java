package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadBytesRangeSLocation extends ReadRange {
    private static final Logger log = LoggerFactory.getLogger(ReadBytesRangeSLocation.class);
    private final Location location;
    private final long contentStart;
    private final long contentEnd;
    private final AtomicInteger skipReadPosition = new AtomicInteger();

    protected ReadBytesRangeSLocation(Location location, long start, long end, ChunkCache chunkCache) {
        super(chunkCache, start);
        this.location = location;
        this.contentStart = start;
        this.contentEnd = end;
    }

    protected ReadBytesRangeSLocation(Location location, long start, long end, ChunkObject chunkObj) {
        super(null, start);
        this.location = location;
        this.contentStart = start;
        this.contentEnd = end;
        this.skipReadPosition.setOpaque((int) contentStart % chunkObj.contentGranularity());
        this.activeChunkRange.compareAndSet(null, makeChunkRange(chunkObj));
    }

    @Override
    public String requestId() {
        return "r-" + requestId + "-rs";
    }

    @Override
    public long contentStartOffset() {
        return contentStart;
    }

    @Override
    public long contentEndOffset() {
        return contentEnd;
    }

    @Override
    public long contentLength() {
        return contentEnd - contentStart;
    }

    public String contentRange() {
        return RangeType.ByteRange + " " + contentStart + "-" + (contentEnd - 1) +
               "/" + (location.logicalOffset() + location.logicalLength());
    }

    @Override
    public RangeType rangeType() {
        return RangeType.ByteRange;
    }

    @Override
    public int skipReadSize(int bufferSize) {
        var skipLeft = skipReadPosition.getOpaque();
        if (skipLeft <= 0) {
            return 0;
        }
        var skip = Math.min(bufferSize, skipLeft);
        skipReadPosition.addAndGet(-skip);
        return skip;
    }

    @Override
    public int skipReadSize() {
        return skipReadPosition.getOpaque();
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
            skipReadPosition.setOpaque((int) contentStart % chunkObj.contentGranularity());
            activeChunkRange.compareAndSet(null, makeChunkRange(chunkObj));
            chunkObj.read(activeChunkRange.getOpaque(), executor, f);
            return f;
        }
        chunkCache.query(location.chunkId).thenApply(c -> {
            try {
                skipReadPosition.setOpaque((int) contentStart % c.contentGranularity());
                activeChunkRange.compareAndSet(null, makeChunkRange(c));
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
        return contentStart >= 0 && contentStart <= contentEnd &&
               contentStart >= location.logicalOffset() &&
               contentEnd <= location.logicalEndOffset() &&
               contentLength() <= location.logicalLength();
    }

    @Override
    public String toString() {
        return new StringBuilder(64).append("content bytes [").append(contentStart).append(",").append(contentEnd).append(")").append(contentEnd - contentStart)
                                    .append(" seg ").append(location.chunkId)
                                    .append("[").append(location.offset).append(",")
                                    .append(location.endOffset()).append(")")
                                    .append(location.length)
                                    .append(" obj[").append(location.logicalOffset()).append(",")
                                    .append(location.logicalEndOffset()).append(")")
                                    .append(location.logicalLength())
                                    .toString();
    }

    private ChunkSegmentRange makeChunkRange(ChunkObject chunkObj) {
        var contentGranularity = chunkObj.contentGranularity();
        var writeGranularity = chunkObj.writeGranularity();

        var startSeg = (int) contentStart / contentGranularity;
        var readStart = startSeg * writeGranularity;

        var endSeg = (int) contentEnd / contentGranularity;
        var endSegOffset = contentEnd % contentGranularity;
        if (endSegOffset > 0) {
            ++endSeg;
        }
        var readEnd = Math.min(endSeg * writeGranularity, chunkObj.chunkLength());
        return new ChunkSegmentRange(this, chunkObj, location, readStart, readEnd);
    }
}
