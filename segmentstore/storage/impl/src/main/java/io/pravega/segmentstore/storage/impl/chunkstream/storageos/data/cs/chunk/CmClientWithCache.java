package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.StreamMirrorChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto.StreamValue;
import com.google.protobuf.UnsafeByteOperations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class CmClientWithCache implements CmClient {
    protected final Map<String, StreamValue> streamValueMap = new HashMap<>(16);
    protected final Map<String, List<StreamProto.StreamDataValue>> streamDataValueMap = new HashMap<>(16);
    protected final ChunkGenerator generator;
    protected final DiskClient diskClient;
    protected final CSConfiguration csConfig;
    protected final ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("cm-client"));

    public CmClientWithCache(CSConfiguration csConfig, ChunkGenerator generator, DiskClient diskClient) {
        this.generator = generator;
        this.diskClient = diskClient;
        this.csConfig = csConfig;
    }

    @Override
    public CompletableFuture<StreamValue> queryStream(String streamName) {
        var f = new CompletableFuture<StreamValue>();
        f.complete(streamValueMap.get(streamName));
        return f;
    }

    @Override
    public CompletableFuture<StreamValue> createStreamIfAbsent(String streamName) {
        var f = new CompletableFuture<StreamValue>();
        f.complete(streamValueMap.putIfAbsent(streamName, StreamValue.newBuilder()
                                                                     .setCreateTag(System.currentTimeMillis())
                                                                     .build()));
        return f;
    }

    @Override
    public CompletableFuture<List<StreamProto.StreamDataValue>> listStreamDataValue(String streamName, long startSequence, int max) {
        var f = new CompletableFuture<List<StreamProto.StreamDataValue>>();
        f.complete(streamDataValueMap.get(streamName));
        return f;
    }

    @Override
    public CompletableFuture<List<StreamChunkObject>> createStreamChunk(String streamId, List<UUID> chunkIds, ChunkConfig chunkConfig) throws IOException {
        var f = new CompletableFuture<List<StreamChunkObject>>();
        scheduledExecutor.schedule(()-> {
            var dataList = streamDataValueMap.get(streamId);
            if (dataList == null) {
                if (streamValueMap.get(streamId) == null) {
                    f.completeExceptionally(new CSException("Stream " + streamId + " is not exist"));
                    return;
                }
                dataList = streamDataValueMap.computeIfAbsent(streamId, k -> new ArrayList<>(1));
            }

            var vChunkInfo = chunkIds.stream().map(id -> {
                                         try {
                                             return generator.createNormalChunk(id.toString(), 0, chunkConfig);
                                         } catch (IOException e) {
                                             throw new CSRuntimeException("allocate chunk error");
                                         }
                                     })
                                     .collect(Collectors.toList());
            synchronized (this) {
                var vChunkObj = new ArrayList<StreamChunkObject>(chunkIds.size());
                for (var i = 0; i < vChunkInfo.size(); ++i) {
                    var chunkId = chunkIds.get(i);
                    var chunkInfo = vChunkInfo.get(i);
                    if (dataList.isEmpty()) {
                        var chunkObj = new StreamMirrorChunkObject(0, chunkId, chunkInfo, 1, diskClient, this, chunkConfig);
                        var dataValue = makeStreamDataValue(chunkId, 0);
                        dataList.add(dataValue);
                        vChunkObj.add(chunkObj);
                        continue;
                    }

                    var last = dataList.get(dataList.size() - 1);
                    if (last.getChunkIds().size() >= CSConfiguration.chunkIdByteCapacityInStreamData()) {
                        var chunkObj = new StreamMirrorChunkObject(dataList.size(), chunkId, chunkInfo, 1, diskClient, this, chunkConfig);
                        var dataValue = makeStreamDataValue(chunkId, dataList.size() - 1);
                        dataList.add(dataValue);
                        vChunkObj.add(chunkObj);
                        continue;
                    }

                    StreamProto.StreamDataValue dataValue = appendChunkIdToStreamDataValue(last, chunkId);
                    dataList.set(dataList.size() - 1, dataValue);
                    var chunkObj = new StreamMirrorChunkObject(dataList.size() - 1,
                                                               chunkId,
                                                               chunkInfo,
                                                               dataValue.getChunkIds().size() / Long.BYTES / 2,
                                                               diskClient,
                                                               this,
                                                               chunkConfig);
                    vChunkObj.add(chunkObj);
                }
                f.complete(vChunkObj);
            }
        }, 100, TimeUnit.MILLISECONDS);
        return f;
    }

    @Override
    public CompletableFuture<Void> truncateStream(String streamId, StreamProto.StreamPosition streamPosition) {
        var f = new CompletableFuture<Void>();
        var old = streamValueMap.computeIfPresent(streamId, (id, v) -> StreamValue.newBuilder()
                                                                                  .setCreateTag(v.getCreateTag())
                                                                                  .setCursor(streamPosition)
                                                                                  .build());
        if (old == null) {
            f.completeExceptionally(new CSException("stream " + streamId + " is not exist"));
        } else {
            f.complete(null);
        }
        return f;
    }

    protected StreamProto.StreamDataValue appendChunkIdToStreamDataValue(StreamProto.StreamDataValue streamDataValue, UUID chunkId) {
        var originChunkBytes = streamDataValue.getChunkIds().size();
        var chunkBytes = new byte[originChunkBytes + Long.BYTES * 2];
        streamDataValue.getChunkIds().copyTo(chunkBytes, 0);
        G.U.putLong(chunkBytes, G.BYTE_ARRAY_BASE_OFFSET + originChunkBytes, chunkId.getMostSignificantBits());
        G.U.putLong(chunkBytes, G.BYTE_ARRAY_BASE_OFFSET + originChunkBytes + Long.BYTES, chunkId.getMostSignificantBits());

        var originLogicalOffsetBytes = streamDataValue.getLogicalOffsets().size();
        var streamLogicalOffsets = new byte[originLogicalOffsetBytes + Long.BYTES];
        G.U.putLong(streamLogicalOffsets, G.BYTE_ARRAY_BASE_OFFSET + originLogicalOffsetBytes, -1);

        var originPhysicalOffsetBytes = streamDataValue.getLogicalOffsets().size();
        var streamPhysicalOffsets = new byte[originPhysicalOffsetBytes + Long.BYTES];
        G.U.putLong(streamPhysicalOffsets, G.BYTE_ARRAY_BASE_OFFSET + originPhysicalOffsetBytes, -1);
        return StreamProto.StreamDataValue.newBuilder()
                                          .setCreateTag(System.currentTimeMillis())
                                          .setChunkIds(UnsafeByteOperations.unsafeWrap(chunkBytes))
                                          .setLogicalOffsets(UnsafeByteOperations.unsafeWrap(streamPhysicalOffsets))
                                          .setPhysicalOffsets(UnsafeByteOperations.unsafeWrap(streamPhysicalOffsets))
                                          .setStartLogicalOffset(-1)
                                          .setEndLogicalOffset(-1)
                                          .setStartPhysicalOffset(-1)
                                          .setEndPhysicalOffset(-1)
                                          .setGoodLogicalOffset(-1)
                                          .setGoodPhysicalOffset(-1)
                                          .build();
    }

    protected StreamProto.StreamDataValue makeStreamDataValue(UUID firstChunkId, int sequence) {
        var chunkBytes = new byte[Long.BYTES * 2];
        G.U.putLong(chunkBytes, G.BYTE_ARRAY_BASE_OFFSET, firstChunkId.getMostSignificantBits());
        G.U.putLong(chunkBytes, G.BYTE_ARRAY_BASE_OFFSET + Long.BYTES, firstChunkId.getMostSignificantBits());
        var streamOffsets = new byte[Long.BYTES];
        G.U.putLong(streamOffsets, G.BYTE_ARRAY_BASE_OFFSET, -1);
        return StreamProto.StreamDataValue.newBuilder()
                                          .setCreateTag(System.currentTimeMillis())
                                          .setChunkIds(UnsafeByteOperations.unsafeWrap(chunkBytes))
                                          .setLogicalOffsets(UnsafeByteOperations.unsafeWrap(streamOffsets))
                                          .setPhysicalOffsets(UnsafeByteOperations.unsafeWrap(streamOffsets))
                                          .setStartLogicalOffset(-1)
                                          .setEndLogicalOffset(-1)
                                          .setStartPhysicalOffset(-1)
                                          .setEndPhysicalOffset(-1)
                                          .setGoodLogicalOffset(-1)
                                          .setGoodPhysicalOffset(-1)
                                          .build();
    }
}
