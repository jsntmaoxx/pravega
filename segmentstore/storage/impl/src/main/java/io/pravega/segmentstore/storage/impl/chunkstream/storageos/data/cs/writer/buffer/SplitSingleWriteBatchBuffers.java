package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.Collections;
import java.util.List;

public class SplitSingleWriteBatchBuffers implements WriteBatchBuffers {
    private final WriteBatchBuffers parent;
    private final int partIndex;
    private final WriteDataBuffer buffer;

    public SplitSingleWriteBatchBuffers(WriteBatchBuffers parent, int partIndex, WriteDataBuffer buffer) {
        this.parent = parent;
        this.partIndex = partIndex;
        this.buffer = buffer;
    }

    @Override
    public boolean add(WriteDataBuffer buffer) {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support add");
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public List<WriteDataBuffer> buffers() {
        return Collections.singletonList(buffer);
    }

    @Override
    public void completeWithException(Throwable t) {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support completeWithException");
    }

    @Override
    public void complete(Location location, ChunkObject chunkObj) {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support complete");
    }

    @Override
    public String requestId() {
        return parent.requestId() + "-sp" + partIndex;
    }

    @Override
    public void seal() {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support seal");
    }

    @Override
    public WriteDataBuffer getBuffer(int i) {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support getBuffer");
    }

    @Override
    public long firstDataBufferCreateNano() {
        return parent.firstDataBufferCreateNano();
    }

    @Override
    public WriteBatchBuffers[] split(int position) throws CSException {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support split");
    }

    @Override
    public void updateEpoch(byte[] epoch) {
        throw new UnsupportedOperationException("SplitSingleWriteBatchBuffers is not support updateEpoch");
    }

    @Override
    public String toString() {
        return "sp" + partIndex + "-" + parent.toString();
    }
}
