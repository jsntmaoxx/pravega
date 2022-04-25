package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.List;

public interface WriteBatchBuffers {
    boolean add(WriteDataBuffer buffer);

    int size();

    List<WriteDataBuffer> buffers();

    void completeWithException(Throwable t);

    void complete(Location location, ChunkObject chunkObj);

    String requestId();

    void seal();

    WriteDataBuffer getBuffer(int i);

    long firstDataBufferCreateNano();

    WriteBatchBuffers[] split(int position) throws CSException;

    void updateEpoch(byte[] epoch);
}
