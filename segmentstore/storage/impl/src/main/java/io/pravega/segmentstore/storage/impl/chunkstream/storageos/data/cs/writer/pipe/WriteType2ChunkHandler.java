package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Amount;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.ArrayWriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec.ECHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class WriteType2ChunkHandler extends WriteChunkHandler {
    private static final Logger log = LoggerFactory.getLogger(WriteType2ChunkHandler.class);
    private static final Duration Chunk2DataBufferCreateToAcceptDuration = Metrics.makeMetric("ChunkII.dataBuffer.create2Accept.duration", Duration.class);
    private static final Duration Chunk2BatchBufferCreateToStartWriteDuration = Metrics.makeMetric("ChunkII.batchBuffer.create2StartWrite.duration", Duration.class);
    private static final Duration Chunk2BatchBufferWriteDiskDuration = Metrics.makeMetric("ChunkII.batchBuffer.writeDisk.duration", Duration.class);
    private static final Amount Chunk2BatchBufferWriteDiskBW = Metrics.makeMetric("ChunkII.batchBuffer.writeDisk.bw", Amount.class);
    private static final Duration Chunk2TriggerWriteDiskDuration = Metrics.makeMetric("ChunkII.triggerWriteDisk.duration", Duration.class);

    private final InputDataChunkFetcher inputDataFetcher;
    private final int maxPrepareWriteBatchCount;
    private final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
    private final ECHandler ecHandler;
    private final int beginWriteOffset;
    private int acceptDataOffset;
    private volatile long logicalStart = Long.MAX_VALUE;
    private volatile long logicalEnd = Long.MIN_VALUE;

    public WriteType2ChunkHandler(ChunkObject chunkObj,
                                  Long codeMatrix,
                                  Long segBuffer,
                                  InputDataChunkFetcher inputDataFetcher,
                                  Executor executor,
                                  ChunkConfig chunkConfig,
                                  int maxPrepareWriteBatchCount) {
        super(chunkObj, executor);
        this.inputDataFetcher = inputDataFetcher;
        this.maxPrepareWriteBatchCount = maxPrepareWriteBatchCount;
        this.ecHandler = ECHandler.createECHandler(this.chunkObj, codeMatrix, segBuffer, chunkConfig, chunkConfig.csConfig.useNativeBuffer());
        this.beginWriteOffset = this.acceptDataOffset = chunkObj.offset();
    }

    /*
    Single thread offer
     */
    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        var len = buffer.size();
        if (disableReceiveBuffer) {
            log().info("reject {} size {} since we disable receive buffer", buffer, buffer.size());
            return false;
        }

        // disable receive buffer if coming buffer exceed chunk size
        if (len + acceptDataOffset > chunkObj.chunkSize()) {
            log().info("reject {} for acceptData {} + buffer {} > chunk {} size {}",
                       buffer, acceptDataOffset, len, chunkObj.chunkIdStr(), chunkObj.chunkSize());
            disableReceiveBuffer = true;
            return false;
        }

        ecHandler.offer(buffer);

        acceptDataOffset += len;
        if (!buffer.isBatchBuffer()) {
            acceptBuffer(buffer);
        } else {
            flushActiveBuffer();
            readyBatches.add(buffer.toBatchBuffer());
        }
        Chunk2DataBufferCreateToAcceptDuration.updateNanoSecond(System.nanoTime() - buffer.createNano());
        return true;
    }

    @Override
    public void end() {
        super.end();
        // cj_todo move end handler to seal, but it requires ecHandler end support multi thread access
        ecHandler.end();
    }

    @Override
    public void errorEnd() {
        disableReceiveBuffer = true;
        if (!writeFuture.isDone()) {
            writeFuture.completeExceptionally(new CSRuntimeException("complete write chunk " + chunkObj.chunkIdStr() + " with errorEnd"));
        }
        ecHandler.errorEnd();
    }

    @Override
    public void cancel() {
        disableReceiveBuffer = true;
        if (!writeFuture.isDone()) {
            writeFuture.completeExceptionally(new CSRuntimeException("complete write chunk " + chunkObj.chunkIdStr() + " by cancel"));
        }
        ecHandler.cancel();
    }

    /*
        WriteChunkHandler
         */
    @Override
    public CompletableFuture<Void> writeChunkFuture() {
        var type2Future = new CompletableFuture<Void>();
        CompletableFuture.allOf(writeFuture, ecHandler.ecFuture()).whenComplete((r, t) -> {
            if (t != null) {
                log.error("write chunk {} failed current offset {}", chunkObj.chunkIdStr(), chunkObj.chunkLength());
                chunkObj.triggerSeal();
                type2Future.completeExceptionally(t);
                return;
            }
            // trigger complete EC to CM
            log().info("chunk {} offset {} trigger complete ec to cm", chunkObj.chunkIdStr(), chunkObj.offset());
            try {
                var start = System.nanoTime();
                chunkObj.completeClientEC(chunkObj.offset(), ecHandler.ecFuture().get()).whenComplete((sr, st) -> {
                    if (st != null) {
                        log.error("chunk {} sealLength {} complete to cm ec encounter error",
                                  chunkObj.chunkIdStr(), chunkObj.sealLength(), st);
                        type2Future.completeExceptionally(st);
                        return;
                    }
                    log.info("chunk {} sealLength {} complete ec to cm success", sr.chunkIdStr(), chunkObj.sealLength());
                    type2Future.complete(null);
                });
            } catch (Exception e) {
                log.error("chunk {} request complete client ec encounter error", chunkObj.chunkIdStr(), e);
                type2Future.completeExceptionally(e);
            }
        });
        return type2Future;
    }

    /*
    Require thread safe - Jetty thread and ChunkWrite response thread will call it
     */
    @Override
    public boolean writeNext() throws CSRuntimeException {
        // have finish write, hit it may due to write error
        if (writeFuture.isDone()) {
            inWrite.setOpaque(false);
            return false;
        }

        if (!inWrite.compareAndSet(false, true)) {
            return requireData();
        }

        var batchBuffer = poll();
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

        final var length = batchBuffer.size();
        var writingOffset = chunkObj.writingOffset();
        log.debug("start write type2 chunk {} [{},{}){} buffer {}",
                  chunkObj.chunkIdStr(), writingOffset, writingOffset + length, length, batchBuffer);
        final var startWrite = System.nanoTime();
        Chunk2BatchBufferCreateToStartWriteDuration.updateNanoSecond(startWrite - batchBuffer.firstDataBufferCreateNano());

        try {
            chunkObj.write(batchBuffer, executor).whenComplete((Location location, Throwable t) -> {
                if (t != null) {
                    disableReceiveBuffer = true;
                    batchBuffer.completeWithException(t);
                    writeFuture.completeExceptionally(t);
                    triggerSealChunk("write encounter error", t);
                    inputDataFetcher.onWriteError(t);
                    return;
                }
                Chunk2BatchBufferWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);
                Chunk2BatchBufferWriteDiskBW.count(batchBuffer.size());
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
            });
        } catch (Throwable t) {
            log.error("send write request type2 chunk {} buffer {} failed", chunkObj.chunkIdStr(), batchBuffer, t);
            batchBuffer.completeWithException(t);

            writeFuture.completeExceptionally(t);
            triggerSealChunk("send write request encounter error", t);
        }
        Chunk2TriggerWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);

        if (requireData()) {
            // fetch data if call from write complete future
            inputDataFetcher.onFetchChunkData();
        }
        return requireData();
    }

    @Override
    public Location getWriteLocation() {
        assert ecHandler.ecFuture().isDone() && writeFuture.isDone();
        assert acceptDataOffset == chunkObj.offset();
        return new Location(chunkObj.chunkUUID(), beginWriteOffset, chunkObj.offset() - beginWriteOffset,
                            logicalStart, logicalEnd - logicalStart);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected boolean finishedAllWrite() {
        return disableReceiveBuffer &&
               !inWrite.getOpaque() &&
               activeBatch.getOpaque().size() == 0 &&
               readyBatches.isEmpty();
    }

    @Override
    protected int acceptDataOffset() {
        return acceptDataOffset;
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

    /*
    Require thread safe - call by writeNext
     */
    @Override
    public boolean requireData() {
        return !disableReceiveBuffer && readyBatches.size() < maxPrepareWriteBatchCount;
    }

    @Override
    public boolean acceptData() {
        return !disableReceiveBuffer;
    }

    @Override
    protected WriteBatchBuffers newBatchBuffer() {
        return new ArrayWriteBatchBuffers();
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
