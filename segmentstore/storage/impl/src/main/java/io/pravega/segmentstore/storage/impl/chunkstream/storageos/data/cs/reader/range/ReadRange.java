package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ReadRange {
    protected static final int printSegCount = 3;
    private static final Logger log = LoggerFactory.getLogger(ReadRange.class);
    protected final ChunkCache chunkCache;
    protected final AtomicLong contentReadPosition;
    protected final AtomicReference<ChunkSegmentRange> activeChunkRange = new AtomicReference<>();
    protected long requestId;

    protected ReadRange(ChunkCache chunkCache, long contentStart) {
        this.chunkCache = chunkCache;
        this.contentReadPosition = new AtomicLong(contentStart);
    }

    public static ReadRange parseObjectRange(HttpServletRequest req, ChunkCache chunkCache) throws CSException, IOException {
        var locationStr = req.getHeader(StringUtils.HeaderEMCExtensionLocation);
        if (locationStr == null) {
            log.error("{} has no location header {}", req.getPathInfo(), StringUtils.HeaderEMCExtensionLocation);
            throw new CSException("no location");
        }
        var locations = LocationCoder.decode(locationStr);
        if (locations.isEmpty()) {
            log.error("{} has empty location {}", req.getPathInfo(), locationStr);
            throw new CSException("empty location");
        }
        var rangeStr = req.getHeader(StringUtils.HeaderRange);
        if (rangeStr == null || rangeStr.isEmpty()) {
            return locations.size() > 1
                   ? new ReadRangeMLocations(locations, chunkCache)
                   : new ReadRangeSLocation(locations.get(0), chunkCache);
        }

        if (!rangeStr.startsWith(RangeType.ByteRange.toString())) {
            log.error("{} has unsupported range type {}", req.getPathInfo(), rangeStr);
            throw new CSException("unsupported ranges type " + rangeStr, S3ErrorCode.InvalidRange);
        }
        var pos = rangeStr.indexOf('-', RangeType.ByteRange.toString().length() + 1);
        if (pos == -1) {
            log.error("{} has unsupported range format {}", req.getPathInfo(), rangeStr);
            throw new CSException("unsupported ranges format " + rangeStr, S3ErrorCode.InvalidRange);
        }

        var contentStart = Long.parseLong(rangeStr.substring(RangeType.ByteRange.toString().length() + 1, pos));
        var contentEnd = Long.parseLong(rangeStr.substring(pos + 1)) + 1;
        return locations.size() > 1
               ? new ReadBytesRangeMLocation(locations, contentStart, contentEnd, chunkCache)
               : new ReadBytesRangeSLocation(locations.get(0), contentStart, contentEnd, chunkCache);
    }

    public static ReadRange parseChunkObjectRange(HttpServletRequest req, ChunkObject chunkObj) throws CSException {
        var chunkLength = chunkObj.chunkLength();
        var writeGranularity = chunkObj.writeGranularity();
        var writeSegCount = chunkLength / writeGranularity + (chunkLength % writeGranularity > 0 ? 1 : 0);
        var contentLength = chunkLength - (long) writeSegCount * CSConfiguration.writeSegmentOverhead();

        var location = new Location(chunkObj.chunkUUID(), 0, chunkObj.chunkLength(), 0, contentLength);
        var rangeStr = req.getHeader(StringUtils.HeaderRange);
        if (rangeStr == null || rangeStr.isEmpty()) {
            return new ReadRangeSLocation(location, chunkObj);
        }

        if (!rangeStr.startsWith(RangeType.ByteRange.toString())) {
            log.error("{} has unsupported range type {}", req.getPathInfo(), rangeStr);
            throw new CSException("unsupported ranges type " + rangeStr, S3ErrorCode.InvalidRange);
        }
        var pos = rangeStr.indexOf('-', RangeType.ByteRange.toString().length() + 1);
        if (pos == -1) {
            log.error("{} has unsupported range format {}", req.getPathInfo(), rangeStr);
            throw new CSException("unsupported ranges format " + rangeStr, S3ErrorCode.InvalidRange);
        }

        var contentStart = Long.parseLong(rangeStr.substring(RangeType.ByteRange.toString().length() + 1, pos));
        var contentEnd = Long.parseLong(rangeStr.substring(pos + 1)) + 1;
        return new ReadBytesRangeSLocation(location, contentStart, contentEnd, chunkObj);
    }

    public void requestId(long requestId) {
        this.requestId = requestId;
    }

    public abstract String requestId();

    public abstract long contentStartOffset();

    public abstract long contentEndOffset();

    public abstract long contentLength();

    public int advanceReadContentSize(int size) {
        var acceptSize = Math.min((int) (contentEndOffset() - contentReadPosition.getOpaque()), size);
        contentReadPosition.addAndGet(acceptSize);
        return acceptSize;
    }

    public String contentRange() {
        throw new UnsupportedOperationException("do not support contentRange");
    }

    public abstract RangeType rangeType();

    public abstract int skipReadSize(int bufferSize);

    public abstract int skipReadSize();

    public int readPosition() {
        var chunkRange = activeChunkRange.getOpaque();
        return chunkRange == null ? -1 : chunkRange.readPosition();
    }

    public long contentReadPosition() {
        return contentReadPosition.getOpaque();
    }

    public void advanceReadChunkPosition(int amount) throws CSException {
        var chunkRange = activeChunkRange.getOpaque();
        if (chunkRange != null) {
            chunkRange.advanceReadPosition(amount);
            return;
        }
        throw new CSException("no active chunk range");
    }

    public abstract CompletableFuture<ReadDataBuffer> read(Executor executor) throws Exception;

    public boolean readAll() {
        assert contentReadPosition.getOpaque() <= contentEndOffset();
        return contentReadPosition.getOpaque() >= contentEndOffset();
    }

    public abstract boolean valid();
}
