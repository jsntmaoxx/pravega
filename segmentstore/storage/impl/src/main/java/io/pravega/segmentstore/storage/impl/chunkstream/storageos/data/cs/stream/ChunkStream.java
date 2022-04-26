package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream;

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write.StreamChunkWriter;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write.StreamWriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write.StreamWriter;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.stream.StreamProto.StreamPosition;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class ChunkStream implements Stream {
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;
    private final CmClient cmClient;
    private final ChunkConfig chunkConfig;
    private final Executor executor;
    private volatile StreamWriter writer;
    private volatile String streamId;
    private volatile StreamProto.StreamValue streamValue;
    private final AtomicLong streamLogicalOffset = new AtomicLong(0);
    private final AtomicLong streamPhysicalOffset = new AtomicLong(0);

    public ChunkStream(CmClient cmClient, ChunkConfig chunkConfig, Executor executor) {
        this.cmClient = cmClient;
        this.chunkConfig = chunkConfig;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Boolean> open(String streamId, boolean create) throws Exception {
        return cmClient.createStreamIfAbsent(streamId).thenApply(v -> {
            this.streamId = streamId;
            this.streamValue = v;
            this.writer = new StreamChunkWriter(streamId,
                                                streamLogicalOffset,
                                                streamPhysicalOffset,
                                                cmClient,
                                                this.chunkConfig,
                                                executor);
            return true;
        });
    }

    @Override
    public CompletableFuture<Long> append(ByteBuffer data) throws CSException {
        return appendStream(data).thenApply(this::toStreamPhysicalOffset);
    }

    public CompletableFuture<StreamPosition> appendStream(ByteBuffer data) throws CSException {
        var dataBuffer =
                new StreamWriteDataBuffer(streamLogicalOffset, streamPhysicalOffset, data, G.genRequestId())
                        .updateHeader()
                        .updateFooterChecksum();
        writer.offer(dataBuffer);
        return dataBuffer.completeFuture();
    }

    @Override
    public CompletableFuture<Void> truncate(long streamOffset) {
        return cmClient.truncateStream(streamId, toStreamPosition(streamOffset));
    }

    @Override
    public CompletableFuture<Pair<Long, ByteBuffer>> read(long streamOffset) {
        return null;
    }

    @Override
    public CompletableFuture<List<Pair<Long, ByteBuffer>>> read(long streamOffset, int maxBytes) {
        return null;
    }


    @Override
    public void close() throws Exception {
        writer.shutdown();
        streamValue = null;
        streamId = null;
    }

    public String getStreamId() {
        return streamId;
    }

    public QueueStats getQueueStatistics() {
        return new QueueStats(0, 0, MAX_APPEND_LENGTH, 0);
    }

    private StreamPosition toStreamPosition(long streamOffset) {
        return null;
    }

    private long toStreamPhysicalOffset(StreamPosition position) {
        return position.getStreamPhysicalOffset();
    }
}
