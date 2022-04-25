package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.List;

public interface ReadDataBuffer {
    static ReadDataBuffer create(int capacity, boolean useNativeBuffer) {
        return useNativeBuffer ? new DirectReadDataBuffer(capacity) : new HeapReadDataBuffer(capacity);
    }

    int readChunkSize() throws CSException;

    ByteBuffer chunkBuffer();

    List<ByteBuffer> contentBuffers();

    boolean parseAndVerifyData();

    void put(ByteBuf in);

    void put(ByteBuffer buffer);

    int writableBytes();

    long nativeDataAddress();
}
