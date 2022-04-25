package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;

import java.util.ArrayList;
import java.util.List;

public class SplitArrayWriteBatchBuffers implements WriteBatchBuffers {
    private final WriteBatchBuffers parent;
    private final int partIndex;
    private final List<WriteDataBuffer> buffers;
    private int size = 0;

    public SplitArrayWriteBatchBuffers(WriteBatchBuffers parent, int partIndex, int capacity) {
        this.parent = parent;
        this.partIndex = partIndex;
        this.buffers = new ArrayList<>(capacity);
    }

    @Override
    public boolean add(WriteDataBuffer buffer) {
        buffers.add(buffer);
        size += buffer.size();
        //cj_todo memory leak, duplicate before add but no free
        return true;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<WriteDataBuffer> buffers() {
        return buffers;
    }

    @Override
    public void completeWithException(Throwable t) {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support completeWithException");
    }

    @Override
    public void complete(Location location, ChunkObject chunkObj) {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support complete");
    }

    @Override
    public String requestId() {
        return parent.requestId() + "-ap" + partIndex;
    }

    @Override
    public void seal() {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support seal");
    }

    @Override
    public WriteDataBuffer getBuffer(int i) {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support getBuffer");
    }

    @Override
    public long firstDataBufferCreateNano() {
        return parent.firstDataBufferCreateNano();
    }

    @Override
    public WriteBatchBuffers[] split(int position) throws CSException {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support split");
    }

    @Override
    public void updateEpoch(byte[] epoch) {
        throw new UnsupportedOperationException("SplitArrayWriteBatchBuffers is not support updateEpoch");
    }

    @Override
    public String toString() {
        return "ap" + partIndex + "-" + parent.toString();
    }
}
