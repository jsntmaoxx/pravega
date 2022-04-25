package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface ChunkCache {
    void setCmClient(CmClient cmClient);

    void put(UUID chunkId, ChunkObject chunkObject);

    ChunkObject get(UUID chunkId);

    ChunkObject remove(UUID chunkId);

    CompletableFuture<ChunkObject> query(UUID chunkId) throws IOException, CSException;

    void clear();

    void shutdown();
}
