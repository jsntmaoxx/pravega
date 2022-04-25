package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Location {
    public final UUID chunkId;
    public final int offset;
    public final int length;
    private long logicalOffset = Long.MAX_VALUE;
    private long logicalLength = 0;

    public Location(UUID chunkId, int offset, int length) {
        this.chunkId = chunkId;
        this.offset = offset;
        this.length = length;
    }

    public Location(UUID chunkId, int offset, int length, long logicalOffset, long logicalLength) {
        this.chunkId = chunkId;
        this.offset = offset;
        this.length = length;
        this.logicalOffset = logicalOffset;
        this.logicalLength = logicalLength;
    }

    public static Location parseFromByteBuffer(ByteBuffer buffer) {
        var uuidLeast = buffer.getLong();
        var uuidMost = buffer.getLong();
        var offset = buffer.getInt();
        var length = buffer.getInt();
        var logicalOffset = buffer.getLong();
        var logicalLength = buffer.getLong();
        return new Location(new UUID(uuidMost, uuidLeast), offset, length, logicalOffset, logicalLength);
    }

    public static int encodeSize() {
        return 8 + 8 + 4 + 4 + 8 + 8;
    }

    public void logicalOffset(long logicalOffset) {
        this.logicalOffset = logicalOffset;
    }

    public long logicalOffset() {
        return logicalOffset;
    }

    public void logicalLength(long logicalLength) {
        this.logicalLength = logicalLength;
    }

    public long logicalLength() {
        return logicalLength;
    }

    public long logicalEndOffset() {
        return logicalOffset + logicalLength;
    }

    public int endOffset() {
        return offset + length;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(chunkId.getLeastSignificantBits()).putLong(chunkId.getMostSignificantBits())
              .putInt(offset).putInt(length).putLong(logicalOffset()).putLong(logicalLength());
    }
}
