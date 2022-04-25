package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkObjectId;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.UUID;

public abstract class AbstractEncodePolicy implements EncodeStrategy {
    protected static final int BUCKET_INDEX_LENGTH = 5;
    protected static final int CONTAINERID_LENGTH = 7;
    protected static final int SIMID_LENGTH = 4;
    protected static final int BLOBID_LENGTH = 16;

    protected abstract String[] parseKey(String key);

    protected abstract ImmutablePair<String[], Integer> parsePrefix(String prefix);

    @Override
    public UUID makeChunkObjectId(int bucketIndex, String key) {
        var parts = parseKey(key);
        return ChunkObjectId.makeChunkObjectId(bucketIndex, Integer.parseUnsignedInt(parts[0], 16), Integer.parseUnsignedInt(parts[1], 16), Long.parseUnsignedLong(parts[2], 16));
    }

    @Override
    public String chunkPrefixFromPrefix(int bucketIndex, String prefix) {
        int part1Length = BUCKET_INDEX_LENGTH + CONTAINERID_LENGTH + 1;
        int part2Length = BUCKET_INDEX_LENGTH + CONTAINERID_LENGTH + SIMID_LENGTH + 2;
        int part3Length = BUCKET_INDEX_LENGTH + CONTAINERID_LENGTH + SIMID_LENGTH + BLOBID_LENGTH + 4;
        Integer length = null;
        if (prefix == null || prefix.isEmpty()) {
            return null;
        }
        if (prefix.startsWith("/")) {
            throw new IllegalArgumentException("key is not expected start with /, please check");
        }
        String[] parts;
        ImmutablePair<String[], Integer> pair = parsePrefix(prefix);
        parts = pair.getLeft();
        length = pair.getRight();

        if (parts.length == 1) {
            return ChunkObjectId.makeChunkObjectId(bucketIndex, Integer.parseUnsignedInt(parts[0], 16), 0, 0L).toString().substring(0, length == null ? part1Length : length);
        } else if (parts.length == 2) {
            return ChunkObjectId.makeChunkObjectId(bucketIndex, Integer.parseUnsignedInt(parts[0], 16), Integer.parseUnsignedInt(parts[1], 16), 0L).toString().substring(0, length == null ? part2Length : length);
        } else {
            return ChunkObjectId.makeChunkObjectId(bucketIndex, Integer.parseUnsignedInt(parts[0], 16), Integer.parseUnsignedInt(parts[1], 16), Long.parseUnsignedLong(parts[2], 16)).toString().substring(0, length == null ? part3Length : length);
        }
    }
}
