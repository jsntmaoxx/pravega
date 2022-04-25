package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.cache;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.C2KCacheStats;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ChunkC2KCache extends ChunkCTCache {
    private static final Duration getChunkDuration = Metrics.makeMetric("chunk.c2k.cache.get.duration", Duration.class);
    private static final Duration putChunkDuration = Metrics.makeMetric("chunk.c2k.cache.put.duration", Duration.class);
    private final Cache<UUID, ChunkObject> cache;
    private final String name;

    public ChunkC2KCache(String name, int capacity, int expireMinutes) {
        this.name = name;
        this.cache = Cache2kBuilder.of(UUID.class, ChunkObject.class)
                                   .name(name)
                                   .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                                   .entryCapacity(capacity)
                                   .build();
        Metrics.makeStatsMetric(name, new C2KCacheStats(this.cache));
    }

    @Override
    public void put(UUID chunkId, ChunkObject chunkObject) {
        var start = System.nanoTime();
        cache.put(chunkId, chunkObject);
        putChunkDuration.updateNanoSecond(System.nanoTime() - start);
    }

    @Override
    public ChunkObject get(UUID chunkId) {
        var start = System.nanoTime();
        var co = cache.peek(chunkId);
        getChunkDuration.updateNanoSecond(System.nanoTime() - start);
        return co;
    }

    @Override
    public ChunkObject remove(UUID chunkId) {
        return cache.peekAndRemove(chunkId);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void shutdown() {
        if (!cache.isClosed()) {
            cache.close();
        }
        Metrics.removeStatsMetric(name);
    }
}
