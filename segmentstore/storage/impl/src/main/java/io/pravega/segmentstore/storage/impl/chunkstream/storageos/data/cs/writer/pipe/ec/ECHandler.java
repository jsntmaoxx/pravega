package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.ForkJoinPoolStats;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.InputDataBufferHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

public abstract class ECHandler implements InputDataBufferHandler {
    private static final Logger log = LoggerFactory.getLogger(ECHandler.class);
    protected static ForkJoinPool executor;
    protected final ChunkObject chunkObject;
    protected final CSConfiguration csConfig;
    protected final ECSchema ecSchema;
    protected Long codeMatrix;
    protected Long segBuffer;

    public ECHandler(Long codeMatrix, Long segBuffer, ChunkConfig chunkConfig, ChunkObject chunkObject) {
        this.codeMatrix = codeMatrix;
        this.segBuffer = segBuffer;
        this.csConfig = chunkConfig.csConfig;
        this.ecSchema = chunkConfig.ecSchema();
        this.chunkObject = chunkObject;
    }

    public static ECHandler createECHandler(ChunkObject chunkObj, Long codeMatrix, Long segBuffer, ChunkConfig chunkConfig, boolean supportNativeBuffer) {
        if (executor == null) {
            executor = new ForkJoinPool(chunkConfig.csConfig.ecThreadNumber(),
                                        new NamedForkJoinWorkerThreadFactory("ec-"),
                                        new DefaultExceptionHandler(log),
                                        true);
            Metrics.makeStatsMetric("forkjoin.pool.ec.io", new ForkJoinPoolStats(executor));
        }
        return chunkConfig.csConfig.skipEC()
               ? new DummyECHandler(chunkObj, codeMatrix, segBuffer, chunkConfig)
               : supportNativeBuffer ? new AsyncStreamECHandler(chunkObj, codeMatrix, segBuffer, executor, chunkConfig)
                                     : new AsyncBatchECHandler(chunkObj, codeMatrix, segBuffer, executor, chunkConfig);
    }

    /*
        Single thread offer
         */
    public abstract boolean offer(WriteBatchBuffers batchBuffer) throws CSException;

    /*
        Single thread offer
         */
    @Override
    public abstract boolean offer(WriteDataBuffer buffer) throws CSException;

    @Override
    public abstract void end();

    @Override
    public abstract void errorEnd();

    public abstract void cancel();

    public void releaseResource() {
        if (segBuffer != null) {
            ecSchema.dataSegmentCache.giveBack(segBuffer);
            segBuffer = null;
        }
        if (codeMatrix != null) {
            ecSchema.codeMatrixCache.giveBack(codeMatrix);
            codeMatrix = null;
        }
    }

    public abstract CompletableFuture<CmMessage.Copy> ecFuture();
}
