package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ReadBytesRangeMLocation extends ReadRange {
    private final List<Location> locations;
    private final long contentStart;
    private final long contentEnd;

    public ReadBytesRangeMLocation(List<Location> locations, long contentStart, long contentEnd, ChunkCache chunkCache) {
        super(chunkCache, 0);
        this.locations = locations;
        this.contentStart = contentStart;
        this.contentEnd = contentEnd;
    }

    @Override
    public String requestId() {
        return null;
    }

    @Override
    public long contentStartOffset() {
        return 0;
    }

    @Override
    public long contentEndOffset() {
        return 0;
    }

    @Override
    public long contentLength() {
        return contentEnd - contentStart;
    }

    @Override
    public String contentRange() {
        var seg = locations.get(locations.size() - 1);
        return RangeType.ByteRange + " " + contentStart + "-" + (contentEnd - 1) + "/" + (seg.logicalOffset() + seg.logicalLength());
    }

    @Override
    public RangeType rangeType() {
        return RangeType.ByteRange;
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
    public int readPosition() {
        return 0;
    }

    @Override
    public void advanceReadChunkPosition(int amount) {

    }

    @Override
    public CompletableFuture<ReadDataBuffer> read(Executor executor) throws Exception {
        return null;
    }

    @Override
    public boolean readAll() {
        return false;
    }

    @Override
    public boolean valid() {
        return false;
    }

    @Override
    public String toString() {
        var locationSize = locations.size();
        var sb = new StringBuilder(64 * (locationSize + 1)).append("content bytes [").append(contentStart).append(",").append(contentEnd).append(")").append(contentEnd - contentStart).append("\n");
        for (var i = 0; i < Math.min(printSegCount, locationSize); ++i) {
            var seg = locations.get(i);
            sb.append("\tseg-").append(i).append(" ").append(seg.chunkId)
              .append("[").append(seg.offset).append(",").append(seg.endOffset()).append(")").append(seg.length)
              .append(" obj[").append(seg.logicalOffset()).append(",").append(seg.logicalEndOffset()).append(")").append(seg.logicalLength()).append("\n");
        }
        // ignore middle locations
        if (locationSize > printSegCount + 2) {
            sb.append("...skip seg-").append(printSegCount).append(" to seg-").append(printSegCount + 1);
        } else if (locationSize > printSegCount + 1) {
            sb.append("...skip seg-").append(printSegCount);
        }
        // print last location
        if (locationSize > printSegCount) {
            var seg = locations.get(locationSize - 1);
            sb.append("\tseg-").append(locationSize - 1).append(" ").append(seg.chunkId)
              .append("[").append(seg.offset).append(",").append(seg.endOffset()).append(")").append(seg.length)
              .append(" obj[").append(seg.logicalOffset()).append(",").append(seg.logicalEndOffset()).append(")").append(seg.logicalLength()).append("\n");
        }
        return sb.toString();
    }
}
