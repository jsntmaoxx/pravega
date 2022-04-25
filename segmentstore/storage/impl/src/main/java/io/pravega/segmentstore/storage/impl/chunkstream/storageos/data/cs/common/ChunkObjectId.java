package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.util.UUID;

public class ChunkObjectId {
    //0x[0]               [0000]   [0000000]    [0000] [0000 0000 0000 0000]
    //  ChunkObjId_prefix bucketId containerId  SimId  BlobId
    public static final long PrefixMostSigMast = 0xf000000000000000L;
    static final long BucketIdMostSigMask = 0x0ffff00000000000L;
    static final long ContainerIdMostSigBitMask = 0x00000fffffff0000L;
    static final long SimIdMostSigMask = 0x000000000000ffffL;

    static final long ContainerIdMostSigValueMask = 0x000000000fffffffL;

    static final byte ContainerIdValueShift = 16;
    static final byte BucketIdValueShift = 44;
    static final byte PrefixValueShift = 60;


    public static UUID makeChunkObjectIdStartKey(long bucketId) {
        return new UUID((BucketIdMostSigMask & (bucketId << BucketIdValueShift)), 0L);
    }

    public static UUID makeChunkObjectIdEndKey(long bucketId) {
        return bucketId < 0xffff ? new UUID((BucketIdMostSigMask & ((bucketId + 1L) << BucketIdValueShift)), 0L)
                                 : new UUID((PrefixMostSigMast & 1L << PrefixValueShift), 0L);
    }

    public static UUID makeChunkObjectId(long bucketId, int containerId, int simId, long blobId) {
        return new UUID((BucketIdMostSigMask & (bucketId << BucketIdValueShift)) |
                        ((ContainerIdMostSigValueMask & containerId) << ContainerIdValueShift) |
                        simId & SimIdMostSigMask,
                        blobId);
    }

    public static UUID randomChunkObjectID(long bucketId) {
        UUID id = UUID.randomUUID();
        return new UUID((id.getMostSignificantBits() & ~(PrefixMostSigMast | BucketIdMostSigMask)) |
                        (BucketIdMostSigMask & (bucketId << BucketIdValueShift)),
                        id.getLeastSignificantBits());
    }

    public static short decodeBucketId(UUID chunkId) throws CSException {
        if ((chunkId.getMostSignificantBits() & PrefixMostSigMast) != 0L) {
            throw new CSException(chunkId + " is not chunkObj Id");
        }
        return (short) ((chunkId.getMostSignificantBits() & BucketIdMostSigMask) >>> BucketIdValueShift);
    }

    public static int decodeContainerId(UUID chunkId) throws CSException {
        if ((chunkId.getMostSignificantBits() & PrefixMostSigMast) != 0L) {
            throw new CSException(chunkId + " is not chunkObj Id");
        }
        return (int) ((chunkId.getMostSignificantBits() & ContainerIdMostSigBitMask) >>> ContainerIdValueShift);
    }

    public static long decodeBlobId(UUID chunkId) throws CSException {
        if ((chunkId.getMostSignificantBits() & PrefixMostSigMast) != 0L) {
            throw new CSException(chunkId + " is not chunkObj Id");
        }
        return chunkId.getLeastSignificantBits();
    }

    public static int decodeSimId(UUID chunkId) throws CSException {
        if ((chunkId.getMostSignificantBits() & PrefixMostSigMast) != 0L) {
            throw new CSException(chunkId + " is not chunkObj Id");
        }
        return (int) (chunkId.getMostSignificantBits() & SimIdMostSigMask);
    }
}
