package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class ChunkObjectKeyGenerator {
    public static String randomChunkObjectKey() {
        var containerId = ThreadLocalRandom.current().nextInt() & 0x0fffffff;
        var blobId = ThreadLocalRandom.current().nextLong();
        var simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return makeChunkObjectKey(containerId, simId, blobId);
    }

    public static String randomChunkObjectKey(int containerId) {
        var blobId = ThreadLocalRandom.current().nextLong();
        var simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        return makeChunkObjectKey(containerId, simId, blobId);
    }

    public static String randomChunkObjectKey(int containerId, int simId) {
        var blobId = ThreadLocalRandom.current().nextLong();
        return makeChunkObjectKey(containerId, simId, blobId);
    }

    public static String makeChunkObjectKey(int containerId, int simId, long blobId) {
        return String.format("%1$07X/%2$04X/%3$016X", containerId, simId, blobId);
    }

    public static String chunkToObjectId(UUID chunkId) throws CSException {
        return makeChunkObjectKey(ChunkObjectId.decodeContainerId(chunkId),
                                  ChunkObjectId.decodeSimId(chunkId),
                                  ChunkObjectId.decodeBlobId(chunkId));
    }

    public static String randomChunkObjectHexKey() {
        var containerId = ThreadLocalRandom.current().nextInt() & 0x0fffffff;
        var blobId = ThreadLocalRandom.current().nextLong();
        var simId = (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE);
        String key = String.format("%1$07X%2$04X%3$016X", containerId, simId, blobId);
        List<String> parts = new ArrayList<>();
        int startIndex = 0;
        while (startIndex != key.length()) {
            int randomNum = startIndex + ThreadLocalRandom.current().nextInt(key.length() - startIndex + 1);
            parts.add("0x" + key.substring(startIndex, randomNum) + "/");
            startIndex = randomNum;
        }
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (ThreadLocalRandom.current().nextInt() % 4 == 0) {
                result.append("xxx/");
            }
            result.append(part);
            if (ThreadLocalRandom.current().nextInt() % 4 == 0) {
                result.append("xxx/");
            }
        }
        return result.substring(0, result.length() - 1);
    }

    public static UUID chunkIdFromChunkObjectKey(Bucket bucket, String key) {
        if (key == null) {
            throw new IllegalArgumentException("null chunk");
        }
        if (key.startsWith("/")) {
            throw new IllegalArgumentException("key is not expected start with /, please check");
        }
        return makeChunkObjectId(bucket, key);
    }

    private static UUID makeChunkObjectId(Bucket bucket, String key) {
        return bucket.getEncodeStrategystrategy().makeChunkObjectId(bucket.index, key);
    }

    public static String chunkPrefixFromPrefix(Bucket bucket, String prefix) {
        return bucket.getEncodeStrategystrategy().chunkPrefixFromPrefix(bucket.index, prefix);
    }

    public static UUID chunkIdFromPrefix(Bucket bucket, String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return null;
        }
        if (prefix.startsWith("/")) {
            throw new IllegalArgumentException("key is not expected start with /, please check");
        }
        var parts = prefix.split("/");
        if (parts.length < 2) {
            return ChunkObjectId.makeChunkObjectId(bucket.index, Integer.parseUnsignedInt(parts[0], 16), (short) ThreadLocalRandom.current().nextInt(Short.MAX_VALUE), ThreadLocalRandom.current().nextLong());
        } else {
            return ChunkObjectId.makeChunkObjectId(bucket.index, Integer.parseUnsignedInt(parts[0], 16), Integer.parseUnsignedInt(parts[1], 16), ThreadLocalRandom.current().nextLong());
        }
    }
}
