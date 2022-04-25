package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.SchemaUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range.ChunkSegmentRange;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class Type1ChunkObject extends AbstractChunkObject {
    private static final Logger log = LoggerFactory.getLogger(Type2ChunkObject.class);
    private static final Duration ChunkIObjResponseToScheduleDuration = Metrics.makeMetric("ChunkIObj.response2Schedule.duration", Duration.class);
    private final ECSchema ecSchema;

    public Type1ChunkObject(UUID chunkId, CmMessage.ChunkInfo chunkInfo, DiskClient rpcClient, CmClient cmClient, ChunkConfig chunkConfig) {
        super(chunkInfo, rpcClient, cmClient, chunkId, chunkConfig.csConfig);
        this.ecSchema = chunkConfig.ecSchema();
    }

    @Override
    protected ECSchema ecSchema() {
        return ecSchema;
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    public UUID chunkUUID() {
        return chunkId;
    }

    @Override
    public String chunkIdStr() {
        return chunkIdStr;
    }

    @Override
    public int sealLength() {
        assert chunkInfo.getStatus() == CmMessage.ChunkStatus.SEALED;
        return chunkInfo.getSealedLength();
    }

    /*
        require thread safe
         */
    @Override
    public CompletableFuture<Location> write(WriteBatchBuffers batchBuffers, Executor executor) throws Exception {
        var f = new CompletableFuture<Location>();
        var copyCount = chunkInfo.getCopiesCount();
        if (copyCount < 3) {
            var sealOffset = dataOffset.getOpaque();
            log().error("Chunk {} only has {} copies, seal it at {}", chunkIdStr, copyCount, sealOffset);
            triggerSeal(sealOffset);
            f.completeExceptionally(new CSException("Chunk " + chunkIdStr + " only has " + copyCount + " copies"));
            return f;
        }
        final var fCopies = new CompletableFuture<?>[copyCount];
        final var size = batchBuffers.size();
        var writeOffset = writingOffset.get();
        while (true) {
            if (!writingOffset.compareAndSet(writeOffset, writeOffset + size)) {
                writeOffset = writingOffset.get();
                continue;
            }
            for (var i = 0; i < copyCount; ++i) {
                var copy = chunkInfo.getCopies(i);
                var seg = copy.getSegments(0);
                var ssLocation = seg.getSsLocation();
                fCopies[i] = rpcClient.write(SchemaUtils.getDeviceId(ssLocation),
                                             batchBuffers.requestId() + "-c" + i,
                                             SchemaUtils.getPartitionId(ssLocation),
                                             ssLocation.getFilename(),
                                             ssLocation.getOffset() + writeOffset,
                                             batchBuffers,
                                             true);
            }
            final var offset = writeOffset;
            CompletableFuture.allOf(fCopies).whenCompleteAsync((r, t) -> {
                if (t != null) {
                    log().error("Write chunk {} [{},{}){} buffer {} failed",
                                chunkIdStr, offset, offset + size, size, batchBuffers, t);
                    f.completeExceptionally(t);
                    return;
                }
                try {
                    for (var fc : fCopies) {
                        var response = (HDDResponse) fc.get();
                        if (!response.success()) {
                            log().error("Write chunk {} [{},{}){} buffer {} copy request {} failed, error {}",
                                        chunkIdStr, offset, offset + size, size, batchBuffers,
                                        response.requestId(), response.errorMessage());
                            f.completeExceptionally(new CSException("Write chunk " + chunkIdStr + " copy " + response.requestId() + "failed"));
                            return;
                        }
                        ChunkIObjResponseToScheduleDuration.updateNanoSecond(System.nanoTime() - response.responseReceiveNano());
                    }
                    if (this.dataOffset.compareAndSet(offset, offset + size)) {
                        f.complete(new Location(chunkId, offset, size));
                    } else {
                        log().error("Write chunk {} [{},{}){} buffer {} update chunk offset from {} to {} failed",
                                    chunkIdStr, offset, offset + size, size, batchBuffers, offset, offset + size);
                        f.completeExceptionally(new CSException("Update chunk " + chunkIdStr + " offset to " + size + " failed"));
                    }
                } catch (Exception e) {
                    log().error("Write chunk {} [{},{}){} buffer {} get result failed",
                                chunkIdStr, offset, offset + size, size, batchBuffers, e);
                }
            }, executor);
            return f;
        }
    }

    @Override
    public void read(ChunkSegmentRange segRange, Executor executor, CompletableFuture<ReadDataBuffer> readFuture) throws Exception {

        // cj_todo implement
    }
}
