package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ChunkSegmentRange {
    private static final AtomicLong segRequestIdGen = new AtomicLong(0);
    private final ReadRange readRange;
    private final ChunkObject chunkObject;
    private final Location location;
    private final AtomicInteger readPos;
    private final int readEnd;

    public ChunkSegmentRange(ReadRange readRange, ChunkObject chunkObject, Location location) {
        this.readRange = readRange;
        this.chunkObject = chunkObject;
        this.location = location;
        this.readPos = new AtomicInteger(location.offset);
        this.readEnd = location.endOffset();
    }

    public ChunkSegmentRange(ReadRange readRange, ChunkObject chunkObject, Location location, int readStart, int readEnd) {
        this.readRange = readRange;
        this.chunkObject = chunkObject;
        this.location = location;
        this.readPos = new AtomicInteger(readStart);
        this.readEnd = readEnd;
    }

    public String nextReadSegRequestId() {
        return readRange.requestId() + "-" + segRequestIdGen.incrementAndGet();
    }

    public int readPosition() {
        return readPos.getOpaque();
    }

    public int readEndOffset() {
        return readEnd;
    }

    public ChunkObject chunkObject() {
        return chunkObject;
    }

    public void advanceReadPosition(int amount) {
        readPos.addAndGet(amount);
    }
}
