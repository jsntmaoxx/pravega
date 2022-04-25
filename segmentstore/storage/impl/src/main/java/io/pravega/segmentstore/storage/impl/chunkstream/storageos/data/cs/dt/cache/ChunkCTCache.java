package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class ChunkCTCache implements ChunkCache {
    private CmClient cmClient;

    @Override
    public void setCmClient(CmClient cmClient) {
        this.cmClient = cmClient;
    }

    @Override
    public CompletableFuture<ChunkObject> query(UUID chunkId) throws IOException, CSException {
        return cmClient.queryChunk(chunkId, true).thenApply(chunkObj -> {
            if (chunkObj != null) {
                put(chunkId, chunkObj);
            }
            return chunkObj;
        });
    }
}
