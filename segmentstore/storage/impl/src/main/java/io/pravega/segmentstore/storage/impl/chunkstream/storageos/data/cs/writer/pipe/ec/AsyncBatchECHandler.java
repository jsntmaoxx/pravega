package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncBatchECHandler extends ECHandler {
    private static final Logger log = LoggerFactory.getLogger(AsyncStreamECHandler.class);
    private static final Duration ecCodeUpdateDuration = Metrics.makeMetric("EC.code.seg.update.duration", Duration.class);
    private static final Duration ecDataChecksumDuration = Metrics.makeMetric("EC.data.seg.checksum.duration", Duration.class);
    private static final Duration ecCodeChecksumDuration = Metrics.makeMetric("EC.code.seg.checksum.duration", Duration.class);
    private static final Duration ecWriteCodeSegDuration = Metrics.makeMetric("EC.w.code.seg.duration", Duration.class);

    private final Executor executor;
    private final Object encodeLock = new Object();
    private final List<CompletableFuture<Void>> ecSubTaskFutures;
    private final CmMessage.Copy.Builder ecCopy;
    private final CompletableFuture<CmMessage.Copy> ecFuture;
    private final AtomicBoolean ended = new AtomicBoolean(false);
    private volatile boolean disableReceiveBuffer = false;
    private volatile boolean aborted = false;

    private int segDataIndex = 0;
    private List<WriteDataBuffer> buffers = new ArrayList<>();
    private long totalBufferSize = 0L;
    private long buffersSize = 0L;

    public AsyncBatchECHandler(ChunkObject chunkObject, Long codeMatrix, Long segBuffer, Executor executor, ChunkConfig chunkConfig) {
        super(codeMatrix, segBuffer, chunkConfig, chunkObject);
        this.executor = executor;
        this.ecFuture = new CompletableFuture<>();
        this.ecSubTaskFutures = new ArrayList<>(this.ecSchema.DataNumber);
        this.ecCopy = this.chunkObject.findECCopy().toBuilder();
    }

    /*
    Single thread offer
     */
    @Override
    public boolean offer(WriteBatchBuffers batchBuffer) {
        if (disableReceiveBuffer) {
            log.error("ec handler of chunk {} skip buffer {} since we have disable accept buffer",
                      chunkObject.chunkIdStr(), batchBuffer);
            return false;
        }

        for (var b : batchBuffer.buffers()) {
            buffers.add(b.duplicate());
        }
        buffersSize += batchBuffer.size();
        totalBufferSize += batchBuffer.size();
        processBuffer();
        return true;
    }

    /*
    Single thread offer
     */
    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        if (disableReceiveBuffer) {
            log.error("ec handler of chunk {} skip buffer {} since we have disable accept buffer",
                      chunkObject.chunkIdStr(), buffer.limit() - buffer.position());
            return false;
        }

        buffers.add(buffer.duplicate());
        buffersSize += buffer.size();
        totalBufferSize += buffer.size();

        processBuffer();
        return true;
    }

    @Override
    public void end() {
        // race condition from type1 chunk
        if (!ended.compareAndSet(false, true)) {
            return;
        }

        disableReceiveBuffer = true;
        if (!buffers.isEmpty()) {
            synchronized (encodeLock) {
                if (segDataIndex >= ecSchema.DataNumber) {
                    throw new CSRuntimeException("sgeDataIndex " + segDataIndex + " is larger than ec schema data number");
                }
                var pickBuffers = new ArrayList<>(buffers);
                buffers.clear();
                ecSubTaskFutures.add(CompletableFuture.runAsync(makeECTask(pickBuffers, segDataIndex++), executor));
            }
        }
        if (!aborted && segDataIndex < ecSchema.DataNumber) {
            // update empty segment checksums
            for (var i = segDataIndex; i < ecSchema.DataNumber; ++i) {
                CmMessage.SegmentInfo seg;
                synchronized (ecCopy) {
                    seg = chunkObject.findCopySegments(ecCopy, i);
                }
                if (seg == null) {
                    log.error("chunk {} encode update code seg {} checksum failed to find segment from copy", chunkObject.chunkIdStr(), i);
                    throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update checksum failed");
                }
                var uSeg = seg.toBuilder().setChecksum(String.valueOf(ecSchema.ChecksumOfPaddingSegment)).build();
                synchronized (ecCopy) {
                    ecCopy.setSegments(i, uSeg);
                }
            }
            log.info("chunk {} skip encode padding segment {} times", chunkObject.chunkIdStr(), ecSchema.DataNumber - segDataIndex);
        }
        ecSubTaskFutures.removeIf(f -> f.isDone() && !f.isCompletedExceptionally());
        if (!ecSubTaskFutures.isEmpty()) {
            CompletableFuture<?>[] fs = new CompletableFuture[ecSubTaskFutures.size()];
            CompletableFuture.allOf(ecSubTaskFutures.toArray(fs)).whenComplete((r, t) -> {
                if (t != null) {
                    log.error("chunk {} wait ec sub tasks encounter error", chunkObject.chunkIdStr(), t);
                    ecFuture.completeExceptionally(t);
                    releaseResource();
                } else {
                    writeEC();
                }
            });
        } else {
            writeEC();
        }
    }

    @Override
    public void errorEnd() {
        if (ecFuture.isDone()) {
            return;
        }
        disableReceiveBuffer = true;
        aborted = true;
        log.warn("chunk {} cancel EC task", chunkObject.chunkIdStr());
        ecSubTaskFutures.removeIf(CompletableFuture::isDone);
        if (!ecSubTaskFutures.isEmpty()) {
            CompletableFuture<?>[] fs = new CompletableFuture[ecSubTaskFutures.size()];
            CompletableFuture.allOf(ecSubTaskFutures.toArray(fs)).whenComplete((r, t) -> {
                if (t != null) {
                    ecFuture.completeExceptionally(new CSRuntimeException("complete EC on chunk " + chunkObject.chunkIdStr() + " with errorEnd", t));
                } else {
                    ecFuture.completeExceptionally(new CSRuntimeException("complete EC on chunk " + chunkObject.chunkIdStr() + " with errorEnd"));
                }
                releaseResource();
            });
        } else {
            ecFuture.completeExceptionally(new CSRuntimeException("complete EC on chunk " + chunkObject.chunkIdStr() + " with errorEnd"));
            releaseResource();
        }
    }

    @Override
    public void cancel() {
        disableReceiveBuffer = true;
        aborted = true;
        for (var f : ecSubTaskFutures) {
            f.cancel(true);
        }
        // don't cancel ecFuture, need wait subTasks before mark it done.
    }

    @Override
    public synchronized void releaseResource() {
        super.releaseResource();
        ecSubTaskFutures.clear();
        buffers.clear();
        segDataIndex = 0;
        buffersSize = 0L;
    }

    @Override
    public CompletableFuture<CmMessage.Copy> ecFuture() {
        return ecFuture;
    }

    private Runnable makeECTask(List<WriteDataBuffer> encodeBuffers, int ecSegIndex) {
        return () -> {
            if (aborted) {
                return;
            }
            try {
                var startUpdateCode = System.nanoTime();
                synchronized (encodeLock) {
                    var mPos = 0;
                    // copy buffer mem to native buffer
                    for (var b : encodeBuffers) {
                        var bp = b.position();
                        var len = b.limit() - bp;
                        if (mPos + len > ecSchema.SegmentLength) {
                            log.error("WSCritical: chunk {} buffers total length {} exceed ec seg {} length {}",
                                      chunkObject.chunkIdStr(), mPos + len, ecSegIndex, ecSchema.SegmentLength);
                            throw new CSRuntimeException(chunkObject.chunkIdStr() + " ec seg buffers length larger than seg length");
                        }
                        // cj_todo improve ec algorithm
                        if (b.isNative()) {
                            G.U.copyMemory(b.address(), segBuffer + mPos, len);
                        } else {
                            G.U.copyMemory(b.array(), G.U.arrayBaseOffset(byte[].class) + bp, null, segBuffer + mPos, len);
                        }
                        mPos += len;
                    }
                    // cj_todo can skip padding 0, need verify by UT first
                    // padding 0
                    if (mPos < ecSchema.SegmentLength) {
                        var len = ecSchema.SegmentLength - mPos;
                        G.U.setMemory(segBuffer + mPos, len, (byte) 0);
                    }
                    // update encode
                    if (ecSchema.SegmentLength != mPos) {
                        log.info("chunk {} encode update seg {} {} byte padding {} byte", chunkObject.chunkIdStr(), ecSegIndex, mPos, ecSchema.SegmentLength - mPos);
                    }
                    var ret = EC.encode_update_seg(ecSchema.DataNumber, ecSchema.CodeNumber, ecSegIndex, ecSchema.SegmentLength, ecSchema.gfTable, segBuffer, codeMatrix);
                    if (!ret) {
                        log.error("chunk {} encode update seg {} failed", chunkObject.chunkIdStr(), ecSegIndex);
                        throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update failed");
                    }
                }
                var startChecksum = System.nanoTime();
                ecCodeUpdateDuration.updateNanoSecond(startChecksum - startUpdateCode);

                CmMessage.SegmentInfo seg;
                synchronized (ecCopy) {
                    seg = chunkObject.findCopySegments(ecCopy, ecSegIndex);
                }
                if (seg == null) {
                    log.error("chunk {} encode update seg {} checksum failed to find segment from copy", chunkObject.chunkIdStr(), ecSegIndex);
                    throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update checksum failed");
                }
                var uSeg = seg.toBuilder()
                              .setChecksum(String.valueOf(CRC.crc32(0, segBuffer, ecSchema.SegmentLength)))
                              .build();
                synchronized (ecCopy) {
                    ecCopy.setSegments(ecSegIndex, uSeg);
                }
                ecDataChecksumDuration.updateNanoSecond(System.nanoTime() - startChecksum);
            } finally {
                for (var b : encodeBuffers) {
                    b.release();
                }
            }
        };
    }

    private void processBuffer() {
        if (buffersSize < ecSchema.SegmentLength) {
            return;
        }
        List<WriteDataBuffer> pickBuffers = null;

        var pickLen = 0;
        int p = 0;
        for (; p < buffers.size(); ++p) {
            var b = buffers.get(p);
            var len = b.size();
            if (pickLen + len >= ecSchema.SegmentLength) {
                if (p == 0 && pickLen + len > ecSchema.SegmentLength) {
                    log.error("WSCritical: chunk {} single buffer size {} large than seg len {}",
                              chunkObject.chunkIdStr(), len, ecSchema.SegmentLength);
                }
                var splitLen = ecSchema.SegmentLength - pickLen;
                if (splitLen == len) {
                    pickBuffers = new ArrayList<>(buffers.subList(0, p + 1));
                    buffers = new ArrayList<>(buffers.subList(p + 1, buffers.size()));
                } else if (splitLen > 0) {
                    pickBuffers = new ArrayList<>(buffers.subList(0, p));
                    var db = b.duplicate();
                    db.limit(db.position() + splitLen);
                    pickBuffers.add(db);
                    b.advancePosition(splitLen);
                    buffers = new ArrayList<>(buffers.subList(p, buffers.size()));
                }
                break;
            } else {
                pickLen += len;
            }
        }

        if (pickBuffers != null) {
            buffersSize -= pickLen;
            synchronized (encodeLock) {
                if (segDataIndex >= ecSchema.DataNumber) {
                    throw new CSRuntimeException("sgeDataIndex " + segDataIndex + " is larger than ec schema data number");
                }
                ecSubTaskFutures.add(CompletableFuture.runAsync(makeECTask(pickBuffers, segDataIndex++), executor));
            }
        }
    }

    private void writeEC() {
        var start = System.nanoTime();
        if (csConfig.skipWriteEC()) {
            releaseResource();
            ecWriteCodeSegDuration.updateNanoSecond(System.nanoTime() - start);
            ecFuture.complete(ecCopy.build());
            return;
        }

        var writeECFutures = new CompletableFuture[ecSchema.CodeNumber * 2];
        try {
            // update checksum
            for (var i = 0; i < ecSchema.CodeNumber; ++i) {
                var index = i;
                var codeSegIndex = ecSchema.DataNumber + i;
                writeECFutures[i] = CompletableFuture.runAsync(() -> {
                    var startChecksum = System.nanoTime();
                    CmMessage.SegmentInfo seg;
                    synchronized (ecCopy) {
                        seg = chunkObject.findCopySegments(ecCopy, codeSegIndex);
                    }
                    if (seg == null) {
                        log.error("chunk {} encode update code seg {} checksum failed to find segment from copy", chunkObject.chunkIdStr(), codeSegIndex);
                        throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update checksum failed");
                    }
                    var uSeg = seg.toBuilder()
                                  .setChecksum(String.valueOf(CRC.crc32(0, ecSchema.codeSegmentAddress(codeMatrix, index), ecSchema.SegmentLength)))
                                  .build();
                    synchronized (ecCopy) {
                        ecCopy.setSegments(codeSegIndex, uSeg);
                    }
                    ecCodeChecksumDuration.updateNanoSecond(System.nanoTime() - startChecksum);
                }, executor);
            }

            var codeSegBufList = ecSchema.codeMatrixCache.toByteBufList(codeMatrix);
            for (var i = 0; i < ecSchema.CodeNumber; ++i) {
                var segBuf = codeSegBufList.get(i);
                var codeSegIndex = ecSchema.DataNumber + i;
                CmMessage.SegmentInfo seg;
                synchronized (ecCopy) {
                    seg = chunkObject.findCopySegments(ecCopy, codeSegIndex);
                }
                writeECFutures[i + ecSchema.CodeNumber] = chunkObject.writeNativeECSegment(seg, codeSegIndex, segBuf, ecSchema.SegmentLength, executor);
            }

            CompletableFuture.allOf(writeECFutures).whenComplete((r, t) -> {
                if (t != null) {
                    log.error("chunk {} write ec code segments failed", chunkObject.chunkIdStr(), t);
                    releaseResource();
                    ecFuture.completeExceptionally(t);
                    return;
                }
                log.info("chunk {} size {} write ec code segments success", chunkObject.chunkIdStr(), totalBufferSize);
                releaseResource();
                ecWriteCodeSegDuration.updateNanoSecond(System.nanoTime() - start);
                ecFuture.complete(ecCopy.build());
            });
        } catch (Exception e) {
            log.error("chunk {} write ec code segments failed", chunkObject.chunkIdStr(), e);
            for (var f : writeECFutures) {
                if (f != null) {
                    f.cancel(true);
                }
            }
            releaseResource();
            ecFuture.completeExceptionally(e);
        }
    }
}
