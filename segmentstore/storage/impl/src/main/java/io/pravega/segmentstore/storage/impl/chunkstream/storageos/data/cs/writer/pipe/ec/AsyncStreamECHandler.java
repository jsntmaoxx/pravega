package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncStreamECHandler extends ECHandler {
    private static final Logger log = LoggerFactory.getLogger(AsyncStreamECHandler.class);
    private static final Duration ecChecksumDuration = Metrics.makeMetric("EC.data.seg.ec.checksum.duration", Duration.class);
    private static final Duration ecCodeChecksumDuration = Metrics.makeMetric("EC.code.seg.checksum.duration", Duration.class);
    private static final Duration ecWriteCodeSegDuration = Metrics.makeMetric("EC.w.code.seg.duration", Duration.class);

    private final Executor executor;
    private final AtomicBoolean processing = new AtomicBoolean(false);
    private final AtomicBoolean allData = new AtomicBoolean(false);
    private final CompletableFuture<CmMessage.Copy> ecFuture;
    private final CmMessage.Copy.Builder ecCopy;
    private final Queue<WriteDataBuffer> bufferQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean disableReceiveBuffer = false;
    private volatile boolean aborted = false;
    private int ecSegIndex = 0;
    private int ecSegOffset = 0;
    private long ecSegChecksum = 0;
    private long buffersSize = 0L;

    public AsyncStreamECHandler(ChunkObject chunkObject, Long codeMatrix, Long segBuffer, Executor executor, ChunkConfig chunkConfig) {
        super(codeMatrix, segBuffer, chunkConfig, chunkObject);
        this.executor = executor;
        this.ecFuture = new CompletableFuture<>();
        this.ecCopy = this.chunkObject.findECCopy().toBuilder();
    }

    @Override
    public boolean offer(WriteBatchBuffers batchBuffer) throws CSException {
        if (disableReceiveBuffer) {
            log.error("ec handler of chunk {} skip buffer {} since we have disable accept buffer",
                      chunkObject.chunkIdStr(), batchBuffer);
            return false;
        }

        for (var b : batchBuffer.buffers()) {
            if (!b.isNative()) {
                throw new CSException("AsyncStreamECHandler require native buffer");
            }
            bufferQueue.add(b.duplicate());
        }
        buffersSize += batchBuffer.size();

        if (processing.compareAndSet(false, true)) {
            executor.execute(new UpdateEC());
        }
        return true;
    }

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        if (disableReceiveBuffer) {
            log.error("ec handler of chunk {} skip buffer {} since we have disable accept buffer",
                      chunkObject.chunkIdStr(), buffer.limit() - buffer.position());
            return false;
        }

        if (!buffer.isNative()) {
            throw new CSException("AsyncStreamECHandler require native buffer");
        }

        bufferQueue.add(buffer.duplicate());
        buffersSize += buffer.size();

        if (processing.compareAndSet(false, true)) {
            executor.execute(new UpdateEC());
        }
        return true;
    }

    @Override
    public void end() {
        if (!allData.compareAndSet(false, true)) {
            return;
        }
        disableReceiveBuffer = true;
        if (processing.compareAndSet(false, true)) {
            executor.execute(new UpdateEC());
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

        // complete feature and release resource if no ec task is running
        if (processing.compareAndSet(false, true)) {
            if (!ecFuture.isDone()) {
                ecFuture.completeExceptionally(new CSException("abort EC on chunk " + chunkObject.chunkIdStr()));
            }
            releaseResource();
        }
    }

    @Override
    public void cancel() {
        disableReceiveBuffer = true;
        aborted = true;

        // complete feature and release resource if no ec task is running
        if (processing.compareAndSet(false, true)) {
            if (!ecFuture.isDone()) {
                ecFuture.completeExceptionally(new CSException("abort EC on chunk " + chunkObject.chunkIdStr()));
            }
            releaseResource();
        }
    }

    @Override
    public synchronized void releaseResource() {
        super.releaseResource();
        while (!bufferQueue.isEmpty()) {
            var b = bufferQueue.poll();
            if (b != null) {
                b.release();
            }
        }
        buffersSize = 0L;
    }

    @Override
    public CompletableFuture<CmMessage.Copy> ecFuture() {
        return ecFuture;
    }

    private class UpdateEC implements Runnable {
        @Override
        public void run() {
            while (true) {
                var buf = bufferQueue.poll();
                if (buf == null) {
                    if (aborted) {
                        if (!ecFuture.isDone()) {
                            ecFuture.completeExceptionally(new CSException("abort EC on chunk " + chunkObject.chunkIdStr()));
                        }
                        releaseResource();
                        break;
                    }

                    // finish EC
                    if (allData.getOpaque()) {
                        updateChecksumOfLastDataSeg();
                        updateChecksumOfLeftDataSegs();
                        writeEC();
                        break;
                    }

                    if (processing.compareAndSet(true, false)) {
                        if (!bufferQueue.isEmpty() && processing.compareAndSet(false, true)) {
                            continue;
                        }
                        if (allData.getOpaque() && processing.compareAndSet(false, true)) {
                            continue;
                        }
                        if (aborted && processing.compareAndSet(false, true)) {
                            continue;
                        }
                        break;
                    }
                    continue;
                }
                if (aborted) {
                    buf.release();
                    continue;
                }

                var success = true;
                try {
                    var start = System.nanoTime();
                    var len = buf.size();
                    if (ecSegOffset + len <= ecSchema.SegmentLength) {
                        success = EC.encode_update_seg_range(ecSchema.DataNumber,
                                                             ecSchema.CodeNumber,
                                                             ecSegIndex,
                                                             ecSegOffset,
                                                             len,
                                                             ecSchema.gfTable,
                                                             buf.address(),
                                                             codeMatrix);
                        ecSegChecksum = CRC.crc32(ecSegChecksum, buf.address(), len);
                        ecSegOffset += len;
                    } else {
                        len = ecSchema.SegmentLength - ecSegOffset;
                        success = EC.encode_update_seg_range(ecSchema.DataNumber,
                                                             ecSchema.CodeNumber,
                                                             ecSegIndex,
                                                             ecSegOffset,
                                                             len,
                                                             ecSchema.gfTable,
                                                             buf.address(),
                                                             codeMatrix);
                        ecSegChecksum = CRC.crc32(ecSegChecksum, buf.address(), len);
                        updateSegChecksum(ecSegIndex, ecSegChecksum);
                        ++ecSegIndex;
                        ecSegChecksum = 0;
                        ecSegOffset = 0;
                        var left = buf.size() - len;

                        success = success && EC.encode_update_seg_range(ecSchema.DataNumber,
                                                                        ecSchema.CodeNumber,
                                                                        ecSegIndex,
                                                                        ecSegOffset,
                                                                        left,
                                                                        ecSchema.gfTable,
                                                                        buf.address() + len,
                                                                        codeMatrix);
                        ecSegChecksum = CRC.crc32(ecSegChecksum, buf.address() + len, left);
                        ecSegOffset = left;
                    }

                    if (ecSegOffset == ecSchema.SegmentLength) {
                        updateSegChecksum(ecSegIndex, ecSegChecksum);
                        ++ecSegIndex;
                        ecSegChecksum = 0;
                        ecSegOffset = 0;
                    }
                    ecChecksumDuration.updateNanoSecond(System.nanoTime() - start);
                } finally {
                    buf.release();
                }

                if (!success) {
                    log.error("async openssl md5 encounter error");
                    aborted = true;
                    ecFuture.completeExceptionally(new CSException("async openssl md5 failed"));
                    break;
                }
                if (ecSegIndex > ecSchema.DataNumber) {
                    log.error("ecSegIndex {} is larger than ec data number {}", ecSegIndex, ecSchema.DataNumber);
                    aborted = true;
                    ecFuture.completeExceptionally(new CSException("async openssl md5 failed"));
                    break;
                }
            }
        }

        private void updateChecksumOfLastDataSeg() {
            if (ecSegOffset < ecSchema.SegmentLength) {
                var len = ecSchema.SegmentLength - ecSegOffset;
                ecSegChecksum = CRC.crc32PaddingZero(ecSegChecksum, len);
                updateSegChecksum(ecSegIndex, ecSegChecksum);
                ++ecSegIndex;
                ecSegChecksum = 0;
                ecSegOffset = 0;
            }
        }

        private void updateChecksumOfLeftDataSegs() {
            // update checksum of left data seg
            if (ecSegIndex < ecSchema.DataNumber) {
                // update empty segment checksums
                for (var i = ecSegIndex; i < ecSchema.DataNumber; ++i) {
                    CmMessage.SegmentInfo seg;
                    synchronized (ecCopy) {
                        seg = chunkObject.findCopySegments(ecCopy, i);
                        if (seg == null) {
                            log.error("chunk {} encode update code seg {} checksum failed to find segment from copy", chunkObject.chunkIdStr(), i);
                            throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update checksum failed");
                        }
                        var uSeg = seg.toBuilder().setChecksum(String.valueOf(ecSchema.ChecksumOfPaddingSegment)).build();
                        ecCopy.setSegments(i, uSeg);
                    }
                }
                log.debug("chunk {} skip encode padding segment {} times", chunkObject.chunkIdStr(), ecSchema.DataNumber - ecSegIndex);
            }
        }

        private void updateSegChecksum(int segIndex, long checksum) {
            synchronized (ecCopy) {
                var seg = chunkObject.findCopySegments(ecCopy, segIndex);
                if (seg == null) {
                    log.error("chunk {} encode update seg {} checksum failed to find segment from copy", chunkObject.chunkIdStr(), segIndex);
                    throw new CSRuntimeException(chunkObject.chunkIdStr() + " encode update checksum failed");
                }
                var uSeg = seg.toBuilder()
                              .setChecksum(String.valueOf(checksum))
                              .build();
                ecCopy.setSegments(segIndex, uSeg);
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
                // update checksum of code seg
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
                    log.info("chunk {} size {} write ec code segments success", chunkObject.chunkIdStr(), buffersSize);
                    releaseResource();
                    ecWriteCodeSegDuration.updateNanoSecond(System.nanoTime() - start);
                    ecFuture.complete(ecCopy.build());
                });
            } catch (Throwable e) {
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
}
