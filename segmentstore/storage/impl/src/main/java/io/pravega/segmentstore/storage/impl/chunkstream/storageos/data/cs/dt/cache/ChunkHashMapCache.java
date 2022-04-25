package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkHashMapCache extends ChunkCTCache {
    private static final Logger log = LoggerFactory.getLogger(ChunkCache.class);
    private final Map<UUID, ChunkObject> cache = new ConcurrentHashMap<>(1024);

    @Override
    public void put(UUID chunkId, ChunkObject chunkObject) {
        cache.put(chunkId, chunkObject);
    }

    @Override
    public ChunkObject get(UUID chunkId) {
        return cache.get(chunkId);
    }

    @Override
    public ChunkObject remove(UUID chunkId) {
        return cache.remove(chunkId);
    }

    @Override
    public void clear() {
        log.info("clear chunk cache size {}", cache.size());
        cache.clear();
    }

    @Override
    public void shutdown() {
    }
}
