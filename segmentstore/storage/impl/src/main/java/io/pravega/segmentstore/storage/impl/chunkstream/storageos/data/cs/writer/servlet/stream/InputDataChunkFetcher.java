package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

public interface InputDataChunkFetcher {
    void onFetchChunkData();

    void onWriteError(Throwable t);
}
