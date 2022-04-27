package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkObjectId;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRecords;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface CmClient {
    static UUID generateChunkId() {
        while (true) {
            UUID cid = UUID.randomUUID();
            if ((cid.getMostSignificantBits() & ChunkObjectId.PrefixMostSigMast) == 0) {
                continue;
            }
            return cid;
        }
    }

    static List<UUID> generateChunkId(int count) {
        var ids = new ArrayList<UUID>(count);
        while (ids.size() < count) {
            UUID cid = UUID.randomUUID();
            if ((cid.getMostSignificantBits() & ChunkObjectId.PrefixMostSigMast) == 0) {
                continue;
            }
            ids.add(cid);
        }
        return ids;
    }

    static UUID generateNoUniqueChunkObjectId(int bucketId) {
        return ChunkObjectId.randomChunkObjectID(bucketId);
    }

    /*
    chunk
     */
    CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity, ChunkConfig chunkConfig, String objName) throws IOException, CSException;

    CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity) throws IOException, CSException;

    CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache, ChunkConfig chunkConfig) throws IOException, CSException;

    CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache) throws IOException, CSException;

    CompletableFuture<Void> deleteChunk(UUID chunkId) throws CSException, IOException;

    CompletableFuture<ChunkObject> sealChunk(ChunkObject chunkObj, int sealLength) throws CSException, IOException;

    CompletableFuture<ChunkObject> completeClientEC(ChunkObject chunkObj, int sealLength, CmMessage.Copy ecCopy) throws CSException, IOException;

    CompletableFuture<ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey>> listChunks(int ctIndex, UUID startAfter, UUID endBefore, int maxKeys, String prefix) throws CSException, IOException;

    /*
    stream
     */
    CompletableFuture<StreamProto.StreamValue> queryStream(String streamName);

    CompletableFuture<StreamProto.StreamValue> createStreamIfAbsent(String streamName);

    CompletableFuture<List<StreamProto.StreamDataValue>> listStreamDataValue(String streamName, long startSequence, int max);

    CompletableFuture<List<StreamChunkObject>> createStreamChunk(String streamId, List<UUID> chunkIds, ChunkConfig chunkConfig) throws IOException;

    CompletableFuture<Void> truncateStream(String streamId, StreamProto.StreamPosition streamOffset);
}
