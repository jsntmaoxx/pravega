package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;

public interface InputDataBufferHandler {

    boolean offer(WriteDataBuffer buffer) throws CSException;

    void end();

    void errorEnd();

    void cancel();
}
