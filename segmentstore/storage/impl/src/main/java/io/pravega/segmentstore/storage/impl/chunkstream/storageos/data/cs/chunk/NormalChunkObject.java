package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range.ChunkSegmentRange;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class NormalChunkObject extends AbstractChunkObject {
    private static final Logger log = LoggerFactory.getLogger(NormalChunkObject.class);
    private static final Duration normalChunkReadSingleSegmentDuration = Metrics.makeMetric("NormalChunk.r.s.seg.duration", Duration.class);
    private static final Duration normalChunkSingleSegReadResponseToScheduleDuration = Metrics.makeMetric("NormalChunk.r.s.seg.response2Schedule.duration", Duration.class);
    private static final Duration normalChunkWriteMultiSegmentDuration = Metrics.makeMetric("NormalChunk.w.m.seg.duration", Duration.class);
    private static final Duration normalChunkMultiSegWriteResponseToScheduleDuration = Metrics.makeMetric("NormalChunk.w.m.seg.response2Schedule.duration", Duration.class);

    private final ECSchema ecSchema;

    public NormalChunkObject(UUID chunkId,
                             CmMessage.ChunkInfo chunkInfo,
                             DiskClient<DiskMessage> rpcClient,
                             CmClient cmClient,
                             ChunkConfig chunkConfig) {
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
    public int sealLength() {
        assert chunkInfo.getStatus() == CmMessage.ChunkStatus.SEALED;
        return chunkInfo.getSealedLength();
    }

    @Override
    public CompletableFuture<Location> write(WriteBatchBuffers batchBuffers, Executor executor) throws Exception {
        var f = new CompletableFuture<Location>();
        var copyCount = chunkInfo.getCopiesCount();

        // cj_todo don't need such check in every write, check after create or query will enough.
//        // check copy count
//        if (copyCount < 3) {
//            var sealOffset = dataOffset.getOpaque();
//            log().error("Chunk {} only has {} copies, seal it at {}", chunkIdStr, copyCount, sealOffset);
//            triggerSeal(sealOffset);
//            f.completeExceptionally(new CSException("Chunk " + chunkIdStr + " only has " + copyCount + " copies"));
//            return f;
//        }
//        // check segment count, should contain either 1 segment or (ecSchema.DataNumber) segments
//        for (var i = 0; i < copyCount; i++) {
//            var copy = chunkInfo.getCopies(i);
//            var segmentsCount = copy.getSegmentsCount();
//            if (segmentsCount != ecSchema.DataNumber && segmentsCount != 1) {
//                var sealOffset = dataOffset.getOpaque();
//                log().error("Chunk {} copy {} has {} segments, seal it at {}", chunkIdStr, i, segmentsCount, sealOffset);
//                triggerSeal(sealOffset);
//                f.completeExceptionally(new CSException("Chunk " + chunkIdStr + " copy " + i + " has " + segmentsCount + " segments"));
//                return f;
//            }
//        }

        batchBuffers.updateEpoch(chunkIdStr.getBytes());

        final var fCopies = new CompletableFuture<?>[copyCount];
        final var size = batchBuffers.size();
        var writeOffset = writingOffset.get();
        while (!writingOffset.compareAndSet(writeOffset, writeOffset + size)) {
            writeOffset = writingOffset.get();
        }

        for (var i = 0; i < copyCount; ++i) {
            var copy = chunkInfo.getCopies(i);
            var segmentsCount = copy.getSegmentsCount();
            if (segmentsCount == 1) {
                // single segment
                fCopies[i] = writeSingleSegmentCopy(batchBuffers, writeOffset, copy, i);
            } else {
                // let write size align with the first write segment, then divide by segmentLength, give us the needed segment count.
                final var needSegmentsCount = (int) Math.ceil((writeOffset % ecSchema().SegmentLength + batchBuffers.size()) * 1.0 / ecSchema().SegmentLength);
                if (needSegmentsCount == 1) {
                    fCopies[i] = writeSingleSegmentCopy(batchBuffers, writeOffset, copy, i);
                } else {
                    // cross segments
                    fCopies[i] = writeCrossSegmentsCopy(batchBuffers, writeOffset, size, copy, i, needSegmentsCount, executor);
                }
            }
        }
        final var offset = writeOffset;
        CompletableFuture.allOf(fCopies).whenCompleteAsync((r, t) -> {
            if (t != null) {
                log().error("Write normal chunk {} [{},{}){} buffer {} failed",
                            chunkIdStr, offset, offset + size, size, batchBuffers, t);
                f.completeExceptionally(t);
                return;
            }
            try {
                var errorCount = 0;
                for (var fc : fCopies) {
                    var response = (DiskMessage) fc.get();
                    if (!response.success()) {
                        ++errorCount;
                        log().error("{} write normal chunk {} [{},{}){} buffer {} failed, error {}",
                                    response.requestId(), chunkIdStr, offset, offset + size, size, batchBuffers, response.errorMessage());
                    }
                }
                if (errorCount > 0) {
                    f.completeExceptionally(new CSException("Write normal chunk " + chunkIdStr + " failed " + errorCount + " copies"));
                }

                if (this.dataOffset.compareAndSet(offset, offset + size)) {
                    log().debug("Write normal chunk {} [{},{}){} buffer {} update chunk offset from {} to {}",
                                chunkIdStr, offset, offset + size, size, batchBuffers, offset, offset + size);
                    f.complete(new Location(chunkId, offset, size));
                } else {
                    log().error("Write normal chunk {} [{},{}){} buffer {} update chunk offset from {} to {} failed. Unexpected race condition.",
                                chunkIdStr, offset, offset + size, size, batchBuffers, offset, offset + size);
                    f.completeExceptionally(new CSException("Update chunk " + chunkIdStr + " offset to " + size + " failed"));
                }
            } catch (Throwable e) {
                log().error("Write normal chunk {} [{},{}){} buffer {} get result failed",
                            chunkIdStr, offset, offset + size, size, batchBuffers, e);
                f.completeExceptionally(e);
            }
        }, executor);
        return f;
    }

    private CompletableFuture<?> writeSingleSegmentCopy(WriteBatchBuffers batchBuffers,
                                                        int writeOffset,
                                                        CmMessage.Copy copy,
                                                        int copyIndex) throws Exception {
        var segIndex = 0;
        var segOffset = writeOffset;
        if (copy.getSegmentsCount() > 1) {
            segIndex = writeOffset / ecSchema.SegmentLength;
            segOffset = writeOffset % ecSchema.SegmentLength;
        }
        var seg = copy.getSegments(segIndex);
        var ssLocation = seg.getSsLocation();
        return rpcClient.write(SchemaUtils.getDeviceId(ssLocation),
                               batchBuffers.requestId() + "-c" + copyIndex,
                               SchemaUtils.getPartitionId(ssLocation),
                               ssLocation.getFilename(),
                               ssLocation.getOffset() + segOffset,
                               batchBuffers,
                               true);
    }

    private CompletableFuture<?> writeCrossSegmentsCopy(WriteBatchBuffers batchBuffers,
                                                        int writeOffset,
                                                        int writeSize,
                                                        CmMessage.Copy copy,
                                                        int copyIndex,
                                                        int needSegmentsCount,
                                                        Executor executor) throws Exception {
        // TODO: Assuming crossing 2 segments at most currently due to SplitArrayWriteBatchBuffers does not implement split method.
        assert needSegmentsCount <= 2;

        var f = new CompletableFuture<>();
        var curWriteOffset = writeOffset;
        var remainingWriteBuffers = batchBuffers;
        final var fSegments = new CompletableFuture<?>[needSegmentsCount];
        final var splitWriteBuffers = new WriteBatchBuffers[needSegmentsCount];
        final var ssLocations = new CmMessage.SSLocation[needSegmentsCount];
        final var segIndices = new int[needSegmentsCount];
        final var segOffsets = new int[needSegmentsCount];
        final var writeSizes = new int[needSegmentsCount];
        var step = 0;
        while (remainingWriteBuffers != null) {
            segIndices[step] = curWriteOffset / ecSchema.SegmentLength;
            segOffsets[step] = curWriteOffset % ecSchema.SegmentLength;
            writeSizes[step] = Math.min(remainingWriteBuffers.size(), ecSchema.SegmentLength - segOffsets[step]);
            if (writeSizes[step] == remainingWriteBuffers.size()) {
                // last segment write, no need to split
                splitWriteBuffers[step] = remainingWriteBuffers;
                remainingWriteBuffers = null;
            } else {
                var batchBuffersParts = remainingWriteBuffers.split(writeSizes[step]);
                splitWriteBuffers[step] = batchBuffersParts[0];
                remainingWriteBuffers = batchBuffersParts[1];
            }
            assert segIndices[step] < ecSchema.DataNumber;
            assert splitWriteBuffers[step].size() == writeSizes[step];

            ssLocations[step] = copy.getSegments(segIndices[step]).getSsLocation();
            fSegments[step] = rpcClient.write(SchemaUtils.getDeviceId(ssLocations[step]),
                                              splitWriteBuffers[step].requestId() + "-" + copyIndex,
                                              SchemaUtils.getPartitionId(ssLocations[step]),
                                              ssLocations[step].getFilename(),
                                              ssLocations[step].getOffset() + segOffsets[step],
                                              splitWriteBuffers[step],
                                              true);
            curWriteOffset += writeSizes[step];
            step++;
        }
        assert step == needSegmentsCount;
        var start = System.nanoTime();
        CompletableFuture.allOf(fSegments)
                         .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                         .whenCompleteAsync((r, t) -> {
                             for (var splitBatch : splitWriteBuffers) {
                                 for (var b : splitBatch.buffers()) {
                                     b.release();
                                 }
                             }
                             if (t != null) {
                                 log().error("Write normal chunk {} [{},{}){} copy {} buffer {} failed",
                                             chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, copyIndex, batchBuffers, t);
                                 f.completeExceptionally(t);
                                 return;
                             }

                             try {
                                 var maxResponseReceiveNano = 0L;
                                 var minRequestCreateNano = Long.MAX_VALUE;
                                 var copyRequestIdBuilder = new StringBuilder("segWriteRequests:");
                                 for (var i = 0; i < needSegmentsCount; i++) {
                                     var fSegment = fSegments[i];
                                     var response = (DiskMessage) fSegment.get();
                                     if (!response.success()) {
                                         log().error("Write normal chunk {} [{},{}){} copy {} buffer {} segment request {} failed, error {} location {} {} {}",
                                                     chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, copyIndex, batchBuffers,
                                                     response.requestId(), response.errorMessage(),
                                                     SchemaUtils.getDeviceId(ssLocations[i]), SchemaUtils.getPartitionId(ssLocations[i]), ssLocations[i].getFilename());
                                         // return failed seg write response as failed copy write response
                                         f.complete(response);
                                         return;
                                     }
                                     maxResponseReceiveNano = Math.max(response.responseReceiveNano(), maxResponseReceiveNano);
                                     minRequestCreateNano = Math.min(response.requestCreateNano(), minRequestCreateNano);
                                     copyRequestIdBuilder.append(response.requestId() + (i == needSegmentsCount - 1 ? "" : ","));
                                 }

                                 var now = System.nanoTime();
                                 normalChunkWriteMultiSegmentDuration.updateNanoSecond(now - start);
                                 normalChunkMultiSegWriteResponseToScheduleDuration.updateNanoSecond(now - maxResponseReceiveNano);

                                 log().debug("Write normal chunk {} [{},{}){} copy {} buffer {} successfully",
                                             chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, copyIndex, batchBuffers);
                                 // combining seg write responses as one copy response returning to upper level
                                 final var finalMinRequestCreateNano = minRequestCreateNano;
                                 final var finalMaxResponseReceiveNano = maxResponseReceiveNano;
                                 final var finalRequestId = copyRequestIdBuilder.toString();
                                 f.complete(new DiskMessage() {
                                     @Override
                                     public String requestId() {
                                         return finalRequestId;
                                     }

                                     @Override
                                     public long requestCreateNano() {
                                         return finalMinRequestCreateNano;
                                     }

                                     @Override
                                     public long responseReceiveNano() {
                                         return finalMaxResponseReceiveNano;
                                     }

                                     @Override
                                     public boolean success() {
                                         return true;
                                     }

                                     @Override
                                     public String errorMessage() {
                                         return null;
                                     }
                                 });
                             } catch (Exception e) {
                                 log().error("Write normal chunk {} [{},{}){} copy {} buffer {} get result failed",
                                             chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, copyIndex, batchBuffers, e);
                                 f.completeExceptionally(e);
                             }
                         }, executor);
        return f;
    }

    @Override
    public CompletableFuture<Void> writeNativeEC(CmMessage.CopyOrBuilder ecCopy, long codeBufferAddress, int codeNumber, int segmentLength, Executor executor) {
        throw new UnsupportedOperationException("NormalChunkObject is not support writeNativeEC");
    }

    @Override
    public void read(ChunkSegmentRange segRange, Executor executor, CompletableFuture<ReadDataBuffer> readFuture) throws Exception {
        final var requestId = segRange.nextReadSegRequestId();
        var copyCount = chunkInfo.getCopiesCount();
        if (copyCount < 3) {
            log().error("{} chunk {} only has {} copies, chunk info {}", requestId, chunkIdStr, copyCount, chunkInfo);
            readFuture.completeExceptionally(new CSException("Chunk " + chunkIdStr + " only has " + copyCount + " copies"));
            return;
        }

        // check segment count, should contain either 1 segment or (ecSchema.DataNumber) segments
        for (var i = 0; i < copyCount; i++) {
            var copy = chunkInfo.getCopies(i);
            var segmentsCount = copy.getSegmentsCount();
            if (segmentsCount != ecSchema.DataNumber && segmentsCount != 1) {
                log().error("{} normal chunk {} copy {} has {} segments, chunk info {}", requestId, chunkIdStr, copy, segmentsCount, chunkInfo);
                readFuture.completeExceptionally(new CSException("Normal chunk " + chunkIdStr + " copy " + i + " has " + segmentsCount + " segments"));
                return;
            }
        }

        var writeGranularity = writeGranularity();
        var start = segRange.readPosition();
        var maxReadWriteInSeg = Math.min(chunkSize() - start, CSConfiguration.readSegmentSize());
        // align with write granularity
        maxReadWriteInSeg = (maxReadWriteInSeg / writeGranularity) * writeGranularity;
        var readLength = Math.min(segRange.readEndOffset() - start, Math.max(writeGranularity, maxReadWriteInSeg));
        if (readLength == 0) {
            log().error("{} chunk {} has no more data to read. chunk sealLength {}, read position {}",
                        requestId, chunkIdStr, sealLength(), segRange.readPosition());
            readFuture.completeExceptionally(new CSException("end of range"));
            return;
        }
        var copyIndex = new Random().nextInt(copyCount);
        // read data from copy random index of copy. if failed, it retries on other copies until all copies ran out.
        readNextCopy(readFuture, requestId, copyIndex, 0, start, readLength, executor);
    }

    private void readCopy(CompletableFuture<ReadDataBuffer> readFuture,
                          String requestId,
                          int copyIndex,
                          int readCopyCount,
                          int start,
                          int readLength,
                          Executor executor) {
        var copy = chunkInfo.getCopies(copyIndex);
        int segCount = copy.getSegmentsCount();
        try {
            readContinousSegment(readFuture, requestId, copyIndex, readCopyCount, start, readLength, executor);
        } catch (Exception e) {
            log.error("{} read normal chunk {} [{}, {}){} copy {} failed", requestId, chunkIdStr, start, start + readLength,
                      readLength, copyIndex, e);
            if (!readFuture.isCancelled() && !readFuture.isDone() && !readFuture.isCompletedExceptionally()) {
                readNextCopy(readFuture, requestId, copyIndex, readCopyCount, start, readLength, executor);
            }
        }
    }

    private void readNextCopy(CompletableFuture<ReadDataBuffer> readFuture,
                              String requestId,
                              int copyIndex,
                              int readCopyCount,
                              int start,
                              int readLength,
                              Executor executor) {
        if (readCopyCount++ >= chunkInfo.getCopiesCount()) {
            log.error("{} reading normal chunk {} [{}, {}) has ran out of copies", requestId, chunkIdStr,
                      start, start + readLength);
            readFuture.completeExceptionally(new CSException("read " + requestId + " failed"));
            return;
        }
        int nextCopyIndex = (copyIndex + readCopyCount) % chunkInfo.getCopiesCount();
        readCopy(readFuture, requestId, nextCopyIndex, readCopyCount, start, readLength, executor);
    }

    private void readContinousSegment(CompletableFuture<ReadDataBuffer> readFuture,
                                      String requestId,
                                      int copyIndex,
                                      int readCopyCount,
                                      int start,
                                      int readLength,
                                      Executor executor) throws Exception {
        var copy = chunkInfo.getCopies(copyIndex);
        int segCount = copy.getSegmentsCount();
        var segIndex = 0;
        var segOffset = start;
        if (segCount == ecSchema.DataNumber) {
            segIndex = start / ecSchema.SegmentLength;
            segOffset = start % ecSchema.SegmentLength;
        }
        var ssLocation = getSegment(copy, segIndex).getSsLocation();
        log.debug("{} normal chunk {} [{}, {}){} start read copy {} seg {} [{}, {}){} location {} {} {}",
                  requestId, chunkIdStr, start, start + readLength, readLength, copyIndex,
                  segIndex, segOffset, segOffset + readLength, readLength,
                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
        var startNano = System.nanoTime();
        readSegment(requestId, ssLocation, segOffset, segOffset + readLength)
                .orTimeout(csConfig.readDiskTimeoutSeconds(), TimeUnit.SECONDS)
                .whenCompleteAsync((response, t) -> {
                    if (t != null) {
                        log.error("{} sub read chunk {} [{}, {}) copy {} failed, location {} {} {}",
                                  requestId, chunkIdStr, start, start + readLength, copyIndex,
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename(), t);
                        readNextCopy(readFuture, requestId, copyIndex, readCopyCount, start, readLength, executor);
                        return;
                    }
                    if (!response.success()) {
                        log.error("{} sub read chunk {} [{}, {}) copy {} response failed, error {} location {} {} {}",
                                  requestId, chunkIdStr, start, start + readLength, copyIndex, response.errorMessage(),
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
                        readNextCopy(readFuture, requestId, copyIndex, readCopyCount, start, readLength, executor);
                        return;
                    }
                    normalChunkReadSingleSegmentDuration.updateNanoSecond(System.nanoTime() - startNano);
                    normalChunkSingleSegReadResponseToScheduleDuration.updateNanoSecond(System.nanoTime() - response.responseReceiveNano());

                    var data = ((ReadResponse) response).data();
                    if (!data.parseAndVerifyData()) {
                        log.error("{} sub read chunk {} [{}, {}) copy {} verify response failed location {} {} {}",
                                  requestId, chunkIdStr, start, start + readLength, copyIndex,
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
                        readNextCopy(readFuture, requestId, copyIndex, readCopyCount, start, readLength, executor);
                    } else {
                        readFuture.complete(data);
                    }
                }, executor);
    }

    private CompletableFuture<? extends DiskMessage> readSegment(String requestId, CmMessage.SSLocation ssLocation, int segOffset, int segEnd) throws Exception {
        return rpcClient.read(SchemaUtils.getDeviceId(ssLocation),
                              requestId,
                              SchemaUtils.getPartitionId(ssLocation),
                              ssLocation.getFilename(),
                              ssLocation.getOffset() + segOffset,
                              segEnd - segOffset);
    }
}
