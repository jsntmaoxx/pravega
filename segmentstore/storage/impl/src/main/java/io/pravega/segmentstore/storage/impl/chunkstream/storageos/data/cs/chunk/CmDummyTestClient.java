package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRecords;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class CmDummyTestClient extends CmClientWithCache {
    private static final Logger log = LoggerFactory.getLogger(CmDummyTestClient.class);

    public CmDummyTestClient(CSConfiguration csConfig, DiskClient diskClient) {
        super(csConfig, new DummyChunkGenerator(csConfig), diskClient);
    }

    @Override
    public CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity, ChunkConfig chunkConfig, String objName) throws IOException {
        if (TestSettings.CreateChunkMaxDelayMs > 0) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(TestSettings.CreateChunkMinDelayMs, TestSettings.CreateChunkMaxDelayMs));
            } catch (InterruptedException ignored) {
            }
        }
        ChunkObject chunkObject;
        switch (chunkType) {
            case Normal:
                chunkObject = new NormalChunkObject(chunkId,
                                                    generator.createNormalChunk(chunkId.toString(), indexGranularity, chunkConfig),
                                                    diskClient,
                                                    this,
                                                    chunkConfig);
                break;
            case I:
            default:
                chunkObject = new Type1ChunkObject(chunkId,
                                                   generator.createType1Chunk(chunkId.toString(), indexGranularity, chunkConfig),
                                                   diskClient,
                                                   this,
                                                   chunkConfig);
                break;
            case II:
                chunkObject = new Type2ChunkObject(chunkId,
                                                   generator.createType2Chunk(chunkId.toString(), indexGranularity, chunkConfig),
                                                   diskClient,
                                                   this,
                                                   chunkConfig);
                break;
        }
        log.info("create test chunk {} type {}", chunkObject.chunkIdStr(), chunkType);
        var createFuture = new CompletableFuture<ChunkObject>();
        createFuture.complete(chunkObject);
        return createFuture;
    }

    @Override
    public CompletableFuture<ChunkObject> createChunk(ChunkType chunkType, UUID chunkId, int indexGranularity) throws IOException, CSException {
        return createChunk(chunkType, chunkId, indexGranularity, csConfig.defaultChunkConfig(), null);
    }

    @Override
    public CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache) throws IOException {
        return queryChunk(chunkId, updateCache, csConfig.defaultChunkConfig());

    }

    @Override
    public CompletableFuture<ChunkObject> queryChunk(UUID chunkId, boolean updateCache, ChunkConfig chunkConfig) throws IOException {
        var f = new CompletableFuture<ChunkObject>();
        f.complete(createSealedChunk(chunkId, ChunkType.II, chunkConfig));
        return f;
    }

    @Override
    public CompletableFuture<Void> deleteChunk(UUID chunkId) {
        var f = new CompletableFuture<Void>();
        f.complete(null);
        return f;
    }

    @Override
    public CompletableFuture<ChunkObject> sealChunk(ChunkObject chunkObj, int sealLength) {
        var f = new CompletableFuture<ChunkObject>();
        chunkObj.chunkInfo(chunkObj.chunkInfo().toBuilder().setSealedLength(sealLength).setStatus(CmMessage.ChunkStatus.SEALED).build());
        f.complete(chunkObj);
        return f;
    }

    @Override
    public CompletableFuture<ChunkObject> completeClientEC(ChunkObject chunkObj, int sealLength, CmMessage.Copy ecCopy) {
        CompletableFuture<ChunkObject> completeEcFuture = new CompletableFuture<>();
        var info = chunkObj.chunkInfo().toBuilder().setSealedLength(sealLength).setStatus(CmMessage.ChunkStatus.SEALED);
        for (var ci = 0; ci < info.getCopiesCount(); ++ci) {
            var c = info.getCopies(ci);
            if (c.getIsEc()) {
                info.setCopies(ci, ecCopy);
                break;
            }
        }
        chunkObj.chunkInfo(info.build());
        completeEcFuture.complete(chunkObj);
        return completeEcFuture;
    }

    @Override
    public CompletableFuture<ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey>> listChunks(int ctIndex, UUID startAfter, UUID endBefore, int maxKeys, String prefix) throws CSException, IOException {
        var f = new CompletableFuture<ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey>>();

        var chunklist = generateChunklist(ctIndex, startAfter, prefix);
        f.complete(ImmutablePair.of(chunklist, null));
        return f;
    }

    protected ChunkObject createSealedChunk(UUID chunkId, ChunkType chunkType, ChunkConfig chunkConfig) throws IOException {
        ChunkObject chunkObject;
        switch (chunkType) {
            case Normal:
                chunkObject = new NormalChunkObject(chunkId,
                                                    generator.createNormalChunk(chunkId.toString(), CSConfiguration.defaultIndexGranularity(), chunkConfig)
                                                             .toBuilder()
                                                             .setStatus(CmMessage.ChunkStatus.SEALED)
                                                             .setSealedLength(chunkConfig.chunkSize())
                                                             .build(),
                                                    diskClient,
                                                    this,
                                                    chunkConfig);
                break;
            case I:
            default:
                chunkObject = new Type1ChunkObject(chunkId,
                                                   generator.createType1Chunk(chunkId.toString(), CSConfiguration.defaultIndexGranularity(), chunkConfig)
                                                            .toBuilder()
                                                            .setStatus(CmMessage.ChunkStatus.SEALED)
                                                            .setSealedLength(chunkConfig.chunkSize())
                                                            .build(),
                                                   diskClient,
                                                   this,
                                                   chunkConfig);
                break;
            case II:
                chunkObject = new Type2ChunkObject(chunkId,
                                                   generator.createType2Chunk(chunkId.toString(), CSConfiguration.defaultIndexGranularity(), chunkConfig)
                                                            .toBuilder()
                                                            .setStatus(CmMessage.ChunkStatus.SEALED)
                                                            .setSealedLength(chunkConfig.chunkSize())
                                                            .build(),
                                                   diskClient,
                                                   this,
                                                   chunkConfig);
                break;
        }
        log.info("create test sealed chunk {} type {}", chunkObject.chunkIdStr(), chunkType);
        return chunkObject;
    }

    private List<DTRecords.DirectoryKVEntry> generateChunklist(int ctIndex, UUID startAfter, String prefix) throws CSException, IOException {
        List<DTRecords.DirectoryKVEntry> chunklist = new ArrayList<>();
        var bucketIndex = Integer.parseUnsignedInt(startAfter.toString().substring(0, 5), 16);
        Bucket bucket = new Bucket("fake-chunk-obj", bucketIndex, csConfig);
        Random rn = new Random();
        int len = rn.nextInt(10) + 1;
        if (ctIndex == 127) {
            len = 1;
        }
        for (int i = 0; i < len; i++) {
            var chunk = ChunkObjectKeyGenerator.chunkIdFromChunkObjectKey(bucket, ChunkObjectKeyGenerator.randomChunkObjectKey());
            CmMessage.ChunkStatus status = CmMessage.ChunkStatus.ACTIVE;
            rn = new Random();
            int number = rn.nextInt(10);
            if (number % 3 == 0) {
                status = CmMessage.ChunkStatus.SEALED;
            }
            number = rn.nextInt(10);
            if (prefix != null && number % 4 == 0) {
                chunk = ChunkObjectKeyGenerator.chunkIdFromPrefix(bucket, prefix.substring(5).replace("-", ""));
            }
            String originalPrefix = prefix == null ? null : prefix.substring(5).replace("-", "");
            if (prefix == null || (prefix != null && chunk.toString().startsWith(prefix))) {
                var kv = DTRecords.DirectoryKVEntry.newBuilder()
                                                   .setSchemaKey(SchemaKeyRecords.SchemaKey.newBuilder()
                                                                                           .setType(SchemaKeyRecords.SchemaKeyType.CHUNK)
                                                                                           .setUserKey(SchemaKeyRecords.ChunkKey.newBuilder()
                                                                                                                                .setChunkIdUuid(SchemaKeyRecords.Uuid.newBuilder()
                                                                                                                                                                     .setHighBytes(chunk.getMostSignificantBits())
                                                                                                                                                                     .setLowBytes(chunk.getLeastSignificantBits()).build()).build().toByteString())
                                                                                           .build())
                                                   .setValue(generator.createType2Chunk(chunk.toString(), CSConfiguration.defaultIndexGranularity(), status, csConfig.defaultChunkConfig(), originalPrefix == null ? "dummyobject-" + i : originalPrefix + "-object-" + i).toByteString())
                                                   .build();
                chunklist.add(kv);
            }
        }
        return chunklist;
    }

}
