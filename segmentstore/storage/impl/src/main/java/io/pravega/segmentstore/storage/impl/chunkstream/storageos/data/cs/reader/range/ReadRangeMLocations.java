package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache.ChunkCache;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadRangeMLocations extends ReadRange {
    private final List<Location> locations;
    private final AtomicInteger activeSegIndex = new AtomicInteger(0);

    public ReadRangeMLocations(List<Location> locations, ChunkCache chunkCache) {
        super(chunkCache, 0);
        this.locations = locations;
    }

    @Override
    public String requestId() {
        return "r-" + requestId + "-s" + activeSegIndex.getOpaque();
    }

    @Override
    public long contentStartOffset() {
        return 0L;
    }

    @Override
    public long contentEndOffset() {
        return locations.get(locations.size() - 1).logicalEndOffset();
    }

    @Override
    public long contentLength() {
        return contentEndOffset() - contentStartOffset();
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
        return null;
    }

    @Override
    public boolean valid() {
        var sl = locations.get(0);
        var el = locations.get(locations.size() - 1);
        return sl.logicalOffset() < sl.offset && el.logicalEndOffset() < el.endOffset();
    }

    @Override
    public String toString() {
        var locationSize = locations.size();
        var sb = new StringBuilder(64 * locationSize);
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
