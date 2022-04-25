package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.ArrayWriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteNormalChunkHandler extends WriteChunkHandler {
    private static final Logger log = LoggerFactory.getLogger(WriteNormalChunkHandler.class);
    private static final Duration NormalChunkDataBufferCreateToAcceptDuration = Metrics.makeMetric("NormalChunk.dataBuffer.create2Accept.duration", Duration.class);
    private static final Duration NormalChunkBatchBufferCreateToStartWriteDuration = Metrics.makeMetric("NormalChunk.batchBuffer.create2StartWrite.duration", Duration.class);
    private static final Duration NormalChunkBatchBufferWriteDiskDuration = Metrics.makeMetric("NormalChunk.batchBuffer.writeDisk.duration", Duration.class);
    private static final Duration NormalChunkTriggerWriteDiskDuration = Metrics.makeMetric("NormalChunk.triggerWriteDisk.duration", Duration.class);
    private final InputDataChunkFetcher inputDataFetcher;
    private final int maxPrepareWriteBatchCount;
    private final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
    private final int beginWriteOffset;
    private AtomicInteger acceptDataOffset = new AtomicInteger(0);
    private volatile long logicalStart = Long.MAX_VALUE;
    private volatile long logicalEnd = Long.MIN_VALUE;

    public WriteNormalChunkHandler(ChunkObject chunkObj,
                                   InputDataChunkFetcher inputDataFetcher,
                                   Executor executor,
                                   CSConfiguration csConfig) {
        super(chunkObj, executor);
        this.inputDataFetcher = inputDataFetcher;
        this.maxPrepareWriteBatchCount = csConfig.maxPrepareWriteBatchCountInChunk();
        this.beginWriteOffset = chunkObj.offset();
        this.acceptDataOffset.setOpaque(chunkObj.offset());
    }

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        var len = buffer.size();
        if (disableReceiveBuffer) {
            log().info("reject {} size {} since we disable receive buffer", buffer, buffer.size());
            return false;
        }

        // check capacity
        int offset;
        do {
            offset = acceptDataOffset.get();
            // disable receive buffer if coming buffer exceed chunk size
            if (len + offset > chunkObj.chunkSize()) {
                disableReceiveBuffer = true;
                log().info("reject {} for acceptData {} + buffer {} > chunk {} size {}",
                           buffer, acceptDataOffset, len, chunkObj.chunkIdStr(), chunkObj.chunkSize());
                return false;
            }
        } while (!acceptDataOffset.compareAndSet(offset, len + offset));

        if (!buffer.isBatchBuffer()) {
            acceptBuffer(buffer);
        } else {
            flushActiveBuffer();
            readyBatches.add(buffer.toBatchBuffer());
        }
        NormalChunkDataBufferCreateToAcceptDuration.updateNanoSecond(System.nanoTime() - buffer.createNano());
        return true;
    }

    @Override
    public void errorEnd() {
        disableReceiveBuffer = true;
        if (!writeFuture.isDone()) {
            writeFuture.completeExceptionally(new CSRuntimeException("complete write chunk " + chunkObj.chunkIdStr() + " with errorEnd"));
        }
    }

    @Override
    public void cancel() {
        disableReceiveBuffer = true;
        if (!writeFuture.isDone()) {
            writeFuture.completeExceptionally(new CSRuntimeException("complete write chunk " + chunkObj.chunkIdStr() + " by cancel"));
        }
    }

    @Override
    protected boolean finishedAllWrite() {
        return disableReceiveBuffer &&
               !inWrite.getOpaque() &&
               activeBatch.get().size() == 0 &&
               readyBatches.isEmpty();
    }

    @Override
    public CompletableFuture<Void> writeChunkFuture() {
        return writeFuture;
    }

    @Override
    public boolean writeNext() {
        // have finish write, hit it may due to write error
        if (writeFuture.isDone()) {
            inWrite.setOpaque(false);
            return false;
        }

        if (!inWrite.compareAndSet(false, true)) {
            return requireData();
        }

        final var batchBuffer = poll();
        if (batchBuffer == null) {
            inWrite.setOpaque(false);

            // complete write chunk successfully
            if (finishedAllWrite()) {
                triggerSealChunk("finish all write in writeNext", null);
                return false;
            }

            // fetch data if call from write complete future
            if (requireData()) {
                inputDataFetcher.onFetchChunkData();
            }
            return requireData();
        }

        log.info("start write normal chunk {} buffer {}", chunkObj.chunkIdStr(), batchBuffer);
        final var startWrite = System.nanoTime();
        NormalChunkBatchBufferCreateToStartWriteDuration.updateNanoSecond(startWrite - batchBuffer.firstDataBufferCreateNano());

        try {
            chunkObj.write(batchBuffer, executor).whenComplete((Location location, Throwable t) -> {
                if (t != null) {
                    disableReceiveBuffer = true;
                    batchBuffer.completeWithException(t);
                    writeFuture.completeExceptionally(t);
                    triggerSealChunk("write chunk error", t);
                    inputDataFetcher.onWriteError(t);
                    return;
                }
                NormalChunkBatchBufferWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);

                try {
                    batchBuffer.complete(location, chunkObj);
                    // cj_todo review these logical offset, it's not right test for each buffer
                    for (var b : batchBuffer.buffers()) {
                        if (b.logicalOffset() < logicalStart) {
                            logicalStart = b.logicalOffset();
                        }
                        if (b.logicalOffset() + b.logicalLength() > logicalEnd) {
                            logicalEnd = b.logicalOffset() + b.logicalLength();
                        }
                    }
                    inWrite.setOpaque(false);
                    writeNext();
                } catch (Throwable e) {
                    log().error("process write normal chunk {} result failed", chunkObj.chunkIdStr(), e);
                    batchBuffer.completeWithException(e);
                    writeFuture.completeExceptionally(e);
                    triggerSealChunk("write chunk error", e);
                    inputDataFetcher.onWriteError(e);
                }
            });
        } catch (Throwable t) {
            log.error("send write request normal chunk {} buffer {} failed", chunkObj.chunkIdStr(), batchBuffer, t);
            batchBuffer.completeWithException(t);
            writeFuture.completeExceptionally(t);
            triggerSealChunk("send write request encounter error", t);
        }
        NormalChunkTriggerWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);

        if (requireData()) {
            // fetch data if call from write complete future
            inputDataFetcher.onFetchChunkData();
        }
        return requireData();
    }

    @Override
    public Location getWriteLocation() {
        assert writeFuture.isDone();
        assert acceptDataOffset.getOpaque() == chunkObj.offset();
        return new Location(chunkObj.chunkUUID(), beginWriteOffset, chunkObj.offset() - beginWriteOffset,
                            logicalStart, logicalEnd - logicalStart);
    }

    @Override
    public boolean requireData() {
        return !disableReceiveBuffer && readyBatches.size() < maxPrepareWriteBatchCount;
    }

    @Override
    public boolean acceptData() {
        return !disableReceiveBuffer;
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected int acceptDataOffset() {
        return acceptDataOffset.getOpaque();
    }

    @Override
    protected void triggerSealChunk(String reason, Throwable t) {
        super.triggerSealChunk(reason, t);
        if (!writeFuture.isDone()) {
            if (t != null) {
                writeFuture.completeExceptionally(t);
            } else {
                writeFuture.complete(null);
            }
        }
    }

    @Override
    protected void acceptBuffer(WriteDataBuffer buffer) {
        // accept the buffer
        var batch = activeBatch.getOpaque();
        if (!batch.add(buffer)) {
            synchronized (activeBatch) {
                while (true) {
                    var ab = activeBatch.getOpaque();
                    if (batch != ab) {
                        if (ab.add(buffer)) {
                            break;
                        }
                        batch = ab;
                    } else {
                        var newBatch = newBatchBuffer();
                        newBatch.add(buffer);
                        activeBatch.setOpaque(newBatch);
                        readyBatches.add(batch);
                        break;
                    }
                }
            }
        }
    }

    @Override
    protected WriteBatchBuffers poll() {
        var batch = readyBatches.poll();
        if (batch == null) {
            synchronized (activeBatch) {
                batch = readyBatches.poll();
                if (batch == null) {
                    var curBatch = activeBatch.getAcquire();
                    if (curBatch.size() > 0) {
                        var newBatch = newBatchBuffer();
                        activeBatch.setOpaque(newBatch);
                        batch = curBatch;
                    }
                }
            }
        }
        if (batch != null) {
            batch.seal();
        }
        return batch;
    }

    @Override
    protected WriteBatchBuffers newBatchBuffer() {
        return new ArrayWriteBatchBuffers();
    }

    protected void flushActiveBuffer() {
        var batch = activeBatch.getOpaque();
        if (batch.size() > 0) {
            synchronized (activeBatch) {
                if (batch == activeBatch.getOpaque()) {
                    activeBatch.setOpaque(newBatchBuffer());
                    readyBatches.add(batch);
                }
            }
        }
    }
}
