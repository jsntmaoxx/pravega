package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;

public interface StreamChunkObject extends ChunkObject {
    long streamSequence();

    int chunkOrder();

    void updateStreamStartOffset(long logicalOffset, long physicalOffset);

    void updateStreamEndOffset(long logicalOffset, long physicalOffset);

    long streamStartLogicalOffset();

    long streamEndLogicalOffset();

    long streamStartPhysicalOffset();

    long streamEndPhysicalOffset();
}
