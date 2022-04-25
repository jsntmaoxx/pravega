package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy;

import java.util.UUID;

public interface EncodeStrategy {
    UUID makeChunkObjectId(int bucketIndex, String key);

    String chunkPrefixFromPrefix(int bucketIndex, String prefix);
}
