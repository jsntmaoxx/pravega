package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;

public interface StreamWriter {
    void offer(StreamWriteDataBuffer data) throws CSException;

    void shutdown();
}
