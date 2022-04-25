package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface WriteDataBuffer<LocationType> {

    byte[] array();

    WriteDataBuffer advancePosition(int len);

    int position();

    WriteDataBuffer limit(int limit);

    int limit();

    WriteDataBuffer truncateAtCurrentDataPosition();

    WriteDataBuffer reset();

    WriteDataBuffer resetData();

    int size();

    boolean hasRemaining();

    WriteDataBuffer duplicate();

    WriteDataBuffer duplicateData();

    WriteDataBuffer updateHeader();

    WriteDataBuffer updateFooterChecksum();

    WriteDataBuffer updateFooterEpoch(byte[] epoch);

    boolean markComplete(Location location, ChunkObject chunkObj);

    boolean markCompleteWithException(Throwable t);

    boolean cancelFuture(boolean canInterrupt);

    CompletableFuture<LocationType> completeFuture();

    boolean isBatchBuffer();

    WriteBatchBuffers toBatchBuffer();

    long createNano();

    long logicalOffset();

    long logicalLength();

    boolean isNative();

    long address();

    void release();

    ByteBuffer shallowByteBuffer();

    ByteBuf wrapToByteBuf();
}
