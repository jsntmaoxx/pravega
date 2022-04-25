package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.LockedArrayWriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec.ECHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteType1ChunkHandler extends WriteChunkHandler {
    private static final Logger log = LoggerFactory.getLogger(WriteType1ChunkHandler.class);
    private static final Duration Type1ChunkDataBufferCreateToAcceptDuration = Metrics.makeMetric("ChunkI.dataBuffer.create2Accept.duration", Duration.class);
    private static final Duration Type1ChunkBatchBufferCreateToStartWriteDuration = Metrics.makeMetric("ChunkI.batchBuffer.create2StartWrite.duration", Duration.class);
    private static final Duration Type1ChunkBatchBufferWriteDiskDuration = Metrics.makeMetric("ChunkI.batchBuffer.writeDisk.duration", Duration.class);
    private static final Duration Type1ChunkTriggerWriteDiskDuration = Metrics.makeMetric("ChunkI.triggerWriteDisk.duration", Duration.class);
    private final InputDataChunkFetcher inputDataFetcher;
    private final AtomicInteger acceptDataOffset = new AtomicInteger(0);
    private final ECHandler ecHandler;

    public WriteType1ChunkHandler(ChunkObject chunkObj,
                                  InputDataChunkFetcher inputDataFetcher,
                                  Executor executor,
                                  Long codeMatrix,
                                  Long segBuffer,
                                  ChunkConfig chunkConfig) {
        super(chunkObj, executor);
        this.inputDataFetcher = inputDataFetcher;
        if (codeMatrix != null && segBuffer != null) {
            this.ecHandler = ECHandler.createECHandler(this.chunkObj, codeMatrix, segBuffer, chunkConfig, false);
        } else {
            this.ecHandler = null;
        }
    }

    /*
     Require thread safe
      */
    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        var len = buffer.size();
        if (disableReceiveBuffer) {
            log().info("{} reject buffer len {} since we disable receive buffer", buffer, len);
            return false;
        }

        // check capacity
        int offset;
        do {
            offset = acceptDataOffset.get();
            // disable receive buffer if coming buffer exceed chunk size
            if (len + offset > chunkObj.chunkSize()) {
                disableReceiveBuffer = true;
                return false;
            }
        } while (!acceptDataOffset.compareAndSet(offset, len + offset));

        // cj_todo starve small buffer in activeBatch?
        if (buffer.isBatchBuffer()) {
            readyBatches.add(buffer.toBatchBuffer());
            Type1ChunkDataBufferCreateToAcceptDuration.updateNanoSecond(System.nanoTime() - buffer.createNano());
            return true;
        }

        acceptBuffer(buffer);
        Type1ChunkDataBufferCreateToAcceptDuration.updateNanoSecond(System.nanoTime() - buffer.createNano());
        return true;
    }

    @Override
    public void errorEnd() {
        disableReceiveBuffer = true;
        throw new UnsupportedOperationException("Don not support call WriteType1ChunkHandler.errorEnd()");
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("Don not support call WriteType1ChunkHandler.cancel()");
    }

    /*
    WriteChunkHandler
     */
    @Override
    public CompletableFuture<Void> writeChunkFuture() {
        throw new UnsupportedOperationException("Don not support call WriteType1ChunkHandler.writeChunkFuture()");
    }

    /*
    Require thread safe - Jetty thread and ChunkWrite response thread will call it
     */
    @Override
    public boolean writeNext() {
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

        log.info("start write type1 chunk {} buffer {}", chunkObj.chunkIdStr(), batchBuffer);
        final var startWrite = System.nanoTime();
        Type1ChunkBatchBufferCreateToStartWriteDuration.updateNanoSecond(startWrite - batchBuffer.firstDataBufferCreateNano());

        try {
            chunkObj.write(batchBuffer, executor).whenComplete((Location location, Throwable t) -> {
                if (t != null) {
                    disableReceiveBuffer = true;
                    batchBuffer.completeWithException(t);
                    triggerSealChunk("write chunk error", t);
                    return;
                }
                Type1ChunkBatchBufferWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);

                try {
                    // cj_todo implement ecHandler support multi-thread
                    if (ecHandler != null) {
                        synchronized (ecHandler) {
                            ecHandler.offer(batchBuffer);
                        }
                    }

                    var tryWrite = inWrite.compareAndSet(true, false);
                    batchBuffer.complete(location, chunkObj);

                    if (tryWrite) {
                        writeNext();
                    }
                } catch (Throwable e) {
                    log().error("handle write result failed ", e);
                    batchBuffer.completeWithException(e);
                }
            });
        } catch (Throwable t) {
            log.error("send write request type1 chunk {} buffer {} failed", chunkObj.chunkIdStr(), batchBuffer, t);
            batchBuffer.completeWithException(t);
            triggerSealChunk("send write request encounter error", t);
        }
        Type1ChunkTriggerWriteDiskDuration.updateNanoSecond(System.nanoTime() - startWrite);

        if (requireData()) {
            // fetch data if call from write complete future
            inputDataFetcher.onFetchChunkData();
        }
        return requireData();
    }

    @Override
    public Location getWriteLocation() {
        return new Location(UUID.randomUUID(), 0, 0);
    }

    @Override
    protected Logger log() {
        return log;
    }

    /*
    Require thread safe - call by writeNext
     */
    @Override
    public boolean requireData() {
        return !disableReceiveBuffer && readyBatches.size() < 2;
    }

    @Override
    public boolean acceptData() {
        return !disableReceiveBuffer;
    }

    @Override
    protected void triggerSealChunk(String reason, Throwable t) {
        super.triggerSealChunk(reason, t);
        if (ecHandler != null) {
            synchronized (ecHandler) {
                ecHandler.end();
            }
        }
    }

    @Override
    protected int acceptDataOffset() {
        return acceptDataOffset.getOpaque();
    }

    @Override
    protected WriteBatchBuffers newBatchBuffer() {
        return new LockedArrayWriteBatchBuffers();
    }

    @Override
    protected boolean finishedAllWrite() {
        return disableReceiveBuffer &&
               !inWrite.getOpaque() &&
               activeBatch.get().size() == 0 &&
               readyBatches.isEmpty();
    }

    @Override
    protected void acceptBuffer(WriteDataBuffer buffer) {
        // accept the buffer
        var batch = activeBatch.getOpaque();
        if (!batch.add(buffer)) {
            var newBatch = newBatchBuffer();
            newBatch.add(buffer);
            while (true) {
                if (activeBatch.compareAndSet(batch, newBatch)) {
                    readyBatches.add(batch);
                    break;
                }
                batch = activeBatch.getOpaque();
                if (batch.add(buffer)) {
                    break;
                }
            }
        }
    }

    @Override
    protected WriteBatchBuffers poll() {
        var batch = readyBatches.poll();
        if (batch == null) {
            var curBatch = activeBatch.getAcquire();
            // the active batch which has buffer may put into ready queue
            batch = readyBatches.poll();
            if (batch == null) {
                // rotate current batch buffer
                if (curBatch.size() > 0) {
                    var newBatch = newBatchBuffer();
                    // offer() could change the activeBatch
                    do {
                        if (activeBatch.compareAndSet(curBatch, newBatch)) {
                            batch = curBatch;
                            break;
                        }
                        // the active batch which has buffer may put into ready queue
                        batch = readyBatches.poll();
                        if (batch != null) {
                            break;
                        }
                        curBatch = activeBatch.getAcquire();
                    } while (curBatch.size() > 0);
                }
            }
        }
        if (batch != null) {
            batch.seal();
        }
        return batch;
    }
}
