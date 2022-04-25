package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.TwoReadDataBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range.ChunkSegmentRange;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.recover.ECSingleSegRangeRecover;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class Type2ChunkObject extends AbstractChunkObject {
    private static final Logger log = LoggerFactory.getLogger(Type2ChunkObject.class);
    private static final Duration type2ChunkWriteSingleSegmentDuration = Metrics.makeMetric("ChunkII.w.s.seg.duration", Duration.class);
    private static final Duration type2ChunkWriteMultiSegmentDuration = Metrics.makeMetric("ChunkII.w.m.seg.duration", Duration.class);
    private static final Duration type2ChunkSingleSegWriteResponseToScheduleDuration = Metrics.makeMetric("ChunkII.w.s.seg.response2Schedule.duration", Duration.class);
    private static final Duration type2ChunkMultiSegWriteResponseToScheduleDuration = Metrics.makeMetric("ChunkII.w.m.seg.response2Schedule.duration", Duration.class);

    private static final Duration type2ChunkReadSingleSegmentDuration = Metrics.makeMetric("ChunkII.r.s.seg.duration", Duration.class);
    private static final Duration type2ChunkReadMultiSegmentDuration = Metrics.makeMetric("ChunkII.r.m.seg.duration", Duration.class);
    private static final Duration type2ChunkSingleSegReadResponseToScheduleDuration = Metrics.makeMetric("ChunkII.r.s.seg.response2Schedule.duration", Duration.class);
    private static final Duration type2ChunkMultiSegReadResponseToScheduleDuration = Metrics.makeMetric("ChunkII.r.m.seg.response2Schedule.duration", Duration.class);
    private final ECSchema ecSchema;

    public Type2ChunkObject(UUID chunkId, CmMessage.ChunkInfo chunkInfo, DiskClient rpcClient, CmClient cmClient, ChunkConfig chunkConfig) {
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

    /*
        multi thread will call it, but assume only one active writing thread
         */
    @Override
    public CompletableFuture<Location> write(WriteBatchBuffers batchBuffers, Executor executor) throws Exception {
        var f = new CompletableFuture<Location>();
        var copyCount = chunkInfo.getCopiesCount();
        if (copyCount < 1) {
            var sealOffset = dataOffset.getOpaque();
            log().error("Chunk {} only has {} copies, seal it at {}, chunk info {}", chunkIdStr, copyCount, sealOffset, chunkInfo);
            triggerSeal(sealOffset);
            f.completeExceptionally(new CSException("Chunk " + chunkIdStr + " only has " + copyCount + " copies"));
            return f;
        }
        var copy = chunkInfo.getCopies(0);
        if (!copy.getIsEc()) {
            var triggered = triggerSeal();
            log().error("Chunk {} has no ec copy, length {}, trigger {} chunk info {}", chunkIdStr, chunkLength(), triggered, chunkInfo);
            f.completeExceptionally(new CSException("Chunk " + chunkIdStr + " has no ec copy"));
            return f;
        }

        batchBuffers.updateEpoch(chunkIdStr.getBytes());

        final var size = batchBuffers.size();
        var writeOffset = writingOffset.get();
        while (!writingOffset.compareAndSet(writeOffset, writeOffset + size)) {
            writeOffset = writingOffset.get();
        }

        var segOffset = writeOffset % ecSchema.SegmentLength;
        if (ecSchema.SegmentLength - segOffset >= size) {
            writeSingleSegment(batchBuffers, writeOffset, size, copy, f, executor);
        } else {
            writeCrossSegment(batchBuffers, writeOffset, size, copy, f, executor);
        }
        return f;
    }

    private void writeSingleSegment(WriteBatchBuffers batchBuffers,
                                    int writeOffset,
                                    int writeSize,
                                    CmMessage.Copy copy,
                                    CompletableFuture<Location> writeFuture,
                                    Executor executor) throws Exception {
        var segIndex = writeOffset / ecSchema.SegmentLength;
        var segOffset = writeOffset % ecSchema.SegmentLength;
        var ssLocation = getSegment(copy, segIndex).getSsLocation();
        var start = System.nanoTime();
        rpcClient.write(SchemaUtils.getDeviceId(ssLocation),
                        batchBuffers.requestId(),
                        SchemaUtils.getPartitionId(ssLocation),
                        ssLocation.getFilename(),
                        ssLocation.getOffset() + segOffset,
                        batchBuffers,
                        true)
                 .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                 .whenCompleteAsync((response, t) -> {
                     if (t != null) {
                         log().error("Write chunk {} [{},{}){} buffer {} failed location {} {} {}",
                                     chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers,
                                     SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename(), t);
                         writeFuture.completeExceptionally(t);
                         return;
                     }
                     try {
                         if (!response.success()) {
                             unsuccessfulWriteResponse(batchBuffers, writeOffset, writeSize, writeFuture, response, ssLocation);
                             return;
                         }
                         var now = System.nanoTime();
                         type2ChunkWriteSingleSegmentDuration.updateNanoSecond(now - start);
                         type2ChunkSingleSegWriteResponseToScheduleDuration.updateNanoSecond(now - response.responseReceiveNano());
                         successfulWriteResponse(batchBuffers, writeOffset, writeSize, writeFuture);
                     } catch (Exception e) {
                         log().error("Write chunk {} [{},{}){} buffer {} get result failed",
                                     chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers, e);
                         writeFuture.completeExceptionally(e);
                     }
                 }, executor);
    }

    private void writeCrossSegment(WriteBatchBuffers batchBuffers,
                                   int writeOffset,
                                   int writeSize,
                                   CmMessage.Copy copy,
                                   CompletableFuture<Location> writeFuture,
                                   Executor executor) throws Exception {
        var segIndex = writeOffset / ecSchema.SegmentLength;
        var segOffset = writeOffset % ecSchema.SegmentLength;
        var batchBuffersParts = batchBuffers.split(ecSchema.SegmentLength - segOffset);
        var ssLocation1 = getSegment(copy, segIndex).getSsLocation();
        var write1 = rpcClient.write(SchemaUtils.getDeviceId(ssLocation1),
                                     batchBuffersParts[0].requestId(),
                                     SchemaUtils.getPartitionId(ssLocation1),
                                     ssLocation1.getFilename(),
                                     ssLocation1.getOffset() + segOffset,
                                     batchBuffersParts[0],
                                     true);
        var ssLocation2 = copy.getSegments(++segIndex).getSsLocation();
        var write2 = rpcClient.write(SchemaUtils.getDeviceId(ssLocation2),
                                     batchBuffersParts[1].requestId(),
                                     SchemaUtils.getPartitionId(ssLocation2),
                                     ssLocation2.getFilename(),
                                     ssLocation2.getOffset(),
                                     batchBuffersParts[1],
                                     true);

        var start = System.nanoTime();
        CompletableFuture.allOf(write1, write2)
                         .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                         .whenCompleteAsync((response, t) -> {
                             for (var splitBatch : batchBuffersParts) {
                                 for (var b : splitBatch.buffers()) {
                                     b.release();
                                 }
                             }
                             if (t != null) {
                                 log().error("Write chunk {} [{},{}){} buffer {} failed location1 {} {} {} location 2 {} {} {}",
                                             chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers,
                                             SchemaUtils.getDeviceId(ssLocation1), SchemaUtils.getPartitionId(ssLocation1), ssLocation1.getFilename(),
                                             SchemaUtils.getDeviceId(ssLocation2), SchemaUtils.getPartitionId(ssLocation2), ssLocation2.getFilename(),
                                             t);
                                 writeFuture.completeExceptionally(t);
                                 return;
                             }
                             try {
                                 var response1 = write1.get();
                                 var response2 = write2.get();
                                 if (!response1.success()) {
                                     unsuccessfulWriteResponse(batchBuffers, writeOffset, writeSize, writeFuture, response1, ssLocation1);
                                     return;
                                 }
                                 if (!response2.success()) {
                                     unsuccessfulWriteResponse(batchBuffers, writeOffset, writeSize, writeFuture, response2, ssLocation2);
                                     return;
                                 }
                                 var now = System.nanoTime();
                                 type2ChunkWriteMultiSegmentDuration.updateNanoSecond(now - start);
                                 type2ChunkMultiSegWriteResponseToScheduleDuration.updateNanoSecond(now - Math.max(response1.responseReceiveNano(), response2.responseReceiveNano()));

                                 successfulWriteResponse(batchBuffers, writeOffset, writeSize, writeFuture);
                             } catch (Exception e) {
                                 log().error("Write chunk {} [{},{}){} buffer {} get result failed",
                                             chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers, e);
                                 writeFuture.completeExceptionally(e);
                             }
                         }, executor);
    }

    private void unsuccessfulWriteResponse(WriteBatchBuffers batchBuffers, int writeOffset, int writeSize,
                                           CompletableFuture<Location> writeFuture, DiskMessage response,
                                           CmMessage.SSLocation ssLocation) {
        log().error("Write chunk {} [{},{}){} buffer {} copy request {} failed, error {} location {} {} {}",
                    chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers,
                    response.requestId(), response.errorMessage(),
                    SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
        writeFuture.completeExceptionally(new CSException("Write chunk " + chunkIdStr + " copy " + response.requestId() + " failed"));
    }

    private void successfulWriteResponse(WriteBatchBuffers batchBuffers, int writeOffset, int writeSize, CompletableFuture<Location> writeFuture) {
        if (this.dataOffset.compareAndSet(writeOffset, writeOffset + writeSize)) {
            log().debug("Write chunk {} [{},{}){} buffer {} update chunk offset from {} to {}",
                        chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers, writeOffset, writeOffset + writeSize);
            writeFuture.complete(new Location(chunkId, writeOffset, writeSize));
        } else {
            log().error("Write chunk {} [{},{}){} buffer {} update chunk offset from {} to {} failed. Unexpected race condition.",
                        chunkIdStr, writeOffset, writeOffset + writeSize, writeSize, batchBuffers, writeOffset, writeOffset + writeSize);
            writeFuture.completeExceptionally(new CSException("Update chunk " + chunkIdStr + " offset to " + writeSize + " failed"));
        }
    }

    @Override
    public void read(ChunkSegmentRange segRange, Executor executor, CompletableFuture<ReadDataBuffer> readFuture) throws Exception {
        final var requestId = segRange.nextReadSegRequestId();
        // cj_todo cache copy?
        var copyCount = chunkInfo.getCopiesCount();
        if (copyCount < 1) {
            log().error("{} chunk {} only has {} copies, chunk info {}", requestId, chunkIdStr, copyCount, chunkInfo);
            readFuture.completeExceptionally(new CSException("Chunk " + chunkIdStr + " only has " + copyCount + " copies"));
            return;
        }
        var copy = chunkInfo.getCopies(0);
        if (!copy.getIsEc()) {
            log().error("{} chunk {} has no ec copy, chunk info {}", requestId, chunkIdStr, chunkInfo);
            readFuture.completeExceptionally(new CSException("Chunk " + chunkIdStr + " has no ec copy"));
            return;
        }
        var writeGranularity = writeGranularity();
        var start = segRange.readPosition();
        var segOffset = start % ecSchema.SegmentLength;
        var maxReadWriteInSeg = Math.min(ecSchema.SegmentLength - segOffset, CSConfiguration.readSegmentSize());
        // align with write granularity
        maxReadWriteInSeg = (maxReadWriteInSeg / writeGranularity) * writeGranularity;
        var readLength = Math.min(segRange.readEndOffset() - start, Math.max(writeGranularity, maxReadWriteInSeg));
        if (readLength == 0) {
            log().error("{} chunk {} has no more data to read. chunk sealLength {}, read position {}", requestId, chunkIdStr, sealLength(), segRange.readPosition());
            readFuture.completeExceptionally(new CSException("end of range"));
            return;
        }
        if (segOffset + readLength <= ecSchema.SegmentLength) {
            readSingleSegment(readFuture, requestId, copy, start, readLength);
        } else {
            readCrossSegment(readFuture, requestId, copy, start, readLength);
        }
    }

    private void readSingleSegment(CompletableFuture<ReadDataBuffer> readFuture,
                                   String requestId,
                                   CmMessage.Copy copy,
                                   int start,
                                   int readLength) throws Exception {
        var segIndex = start / ecSchema.SegmentLength;
        var segOffset = start % ecSchema.SegmentLength;
        var end = start + readLength;
        var ssLocation = getSegment(copy, segIndex).getSsLocation();
        log.debug("{} chunk {} [{}, {}){} start read seg {} [{}, {}){} location {} {} {}",
                  requestId, chunkIdStr, start, end, end - start,
                  segIndex, segOffset, segOffset + readLength, readLength,
                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
        var startNano = System.nanoTime();
        readSegment(requestId, ssLocation, segOffset, segOffset + readLength)
                .orTimeout(csConfig.readDiskTimeoutSeconds(), TimeUnit.SECONDS)
                .whenCompleteAsync((response, t) -> {
                    if (t != null) {
                        log.error("{} sub read chunk {} [{}, {}) failed, location {} {} {}, trigger sync recover",
                                  requestId, chunkIdStr, start, end,
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename(), t);
                        syncRecover(readFuture, requestId, copy, start, readLength, segIndex, segOffset, end, ssLocation);
                        return;
                    }
                    if (!response.success()) {
                        log.error("{} sub read chunk {} [{}, {}) response failed, error {} location {} {} {}",
                                  requestId, chunkIdStr, start, end, response.errorMessage(),
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
                        syncRecover(readFuture, requestId, copy, start, readLength, segIndex, segOffset, end, ssLocation);
                        return;
                    }
                    type2ChunkReadSingleSegmentDuration.updateNanoSecond(System.nanoTime() - startNano);
                    type2ChunkSingleSegReadResponseToScheduleDuration.updateNanoSecond(System.nanoTime() - response.responseReceiveNano());

                    var data = ((ReadResponse) response).data();
                    if (!data.parseAndVerifyData()) {
                        log.error("{} sub read chunk {} [{}, {}) verify response failed location {} {} {}",
                                  requestId, chunkIdStr, start, end,
                                  SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
                        syncRecover(readFuture, requestId, copy, start, readLength, segIndex, segOffset, end, ssLocation);
                    } else {
                        readFuture.complete(data);
                    }
                });
    }

    private void readCrossSegment(CompletableFuture<ReadDataBuffer> readFuture,
                                  String requestId,
                                  CmMessage.Copy copy,
                                  int start,
                                  int readLength) throws Exception {
        var end = start + readLength;
        var segIndex = start / ecSchema.SegmentLength;
        var segOffset = start % ecSchema.SegmentLength;
        var startNano = System.nanoTime();

        var subRequestId1 = requestId + "-1";
        var ssLocation1 = getSegment(copy, segIndex).getSsLocation();
        var segPos1 = segOffset;
        var segEnd1 = ecSchema.SegmentLength;
        var readFuture1 = readSegment(subRequestId1, ssLocation1, segPos1, segEnd1);

        var subRequestId2 = requestId + "-2";
        var ssLocation2 = getSegment(copy, segIndex + 1).getSsLocation();
        var segPos2 = 0;
        var segEnd2 = readLength - (segEnd1 - segPos1);
        var readFuture2 = readSegment(subRequestId2, ssLocation2, segPos2, segEnd2);

        CompletableFuture.allOf(readFuture1, readFuture2)
                         .orTimeout(csConfig.readDiskTimeoutSeconds(), TimeUnit.SECONDS)
                         .whenCompleteAsync((r, t) -> {
                             if (t != null) {
                                 log().error("{} Read chunk {} [{},{}){} failed location1 {} {} {} location 2 {} {} {}",
                                             requestId, chunkIdStr, start, end, readLength,
                                             SchemaUtils.getDeviceId(ssLocation1), SchemaUtils.getPartitionId(ssLocation1), ssLocation1.getFilename(),
                                             SchemaUtils.getDeviceId(ssLocation2), SchemaUtils.getPartitionId(ssLocation2), ssLocation2.getFilename(),
                                             t);
                                 readFuture.completeExceptionally(new CSException("read " + requestId + " failed", t));
                                 return;
                             }
                             try {
                                 var maxResponseReceiveNano = Long.MIN_VALUE;
                                 TwoReadDataBuffers buffers = new TwoReadDataBuffers();

                                 var readResponse1 = (ReadResponse) readFuture1.get();
                                 if (!readResponse1.success()) {
                                     unsuccessfulReadResponse(start, readLength, readFuture, readResponse1, ssLocation1);
                                     return;
                                 }
                                 if (maxResponseReceiveNano < readResponse1.responseReceiveNano()) {
                                     maxResponseReceiveNano = readResponse1.responseReceiveNano();
                                 }
                                 buffers.setBuffer1(readResponse1.data());

                                 var readResponse2 = (ReadResponse) readFuture2.get();
                                 if (!readResponse2.success()) {
                                     unsuccessfulReadResponse(start, readLength, readFuture, readResponse2, ssLocation2);
                                     return;
                                 }
                                 if (maxResponseReceiveNano < readResponse2.responseReceiveNano()) {
                                     maxResponseReceiveNano = readResponse2.responseReceiveNano();
                                 }
                                 buffers.setBuffer2(readResponse2.data());

                                 type2ChunkReadMultiSegmentDuration.updateNanoSecond(System.nanoTime() - startNano);
                                 type2ChunkMultiSegReadResponseToScheduleDuration.updateNanoSecond(System.nanoTime() - maxResponseReceiveNano);
                                 if (!buffers.parseAndVerifyData()) {
                                     log.error("{} sub read chunk {} [{}, {}) verify responses failed",
                                               requestId, chunkIdStr, start, end);
                                     readFuture.completeExceptionally(new CSException("read " + requestId + " verify response failed"));
                                 } else {
                                     readFuture.complete(buffers);
                                 }
                             } catch (Exception e) {
                                 log.error("{} sub read chunk {} [{}, {}) handle failed",
                                           requestId, chunkIdStr, start, end, e);
                                 readFuture.completeExceptionally(new CSException("read " + requestId + " failed", e));
                             }
                         });
    }

    private void unsuccessfulReadResponse(int readOffset, int readSize,
                                          CompletableFuture<ReadDataBuffer> readFuture, DiskMessage response,
                                          CmMessage.SSLocation ssLocation) {
        log().error("read chunk {} [{},{}){} request {} failed, error {} location {} {} {}",
                    chunkIdStr, readOffset, readOffset + readSize, readSize,
                    response.requestId(), response.errorMessage(),
                    SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename());
        readFuture.completeExceptionally(new CSException("Read chunk " + chunkIdStr + " " + response.requestId() + " failed"));
    }

    private CompletableFuture<? extends DiskMessage> readSegment(String requestId, CmMessage.SSLocation ssLocation, int segOffset, int segEnd) throws Exception {
        return rpcClient.read(SchemaUtils.getDeviceId(ssLocation),
                              requestId,
                              SchemaUtils.getPartitionId(ssLocation),
                              ssLocation.getFilename(),
                              ssLocation.getOffset() + segOffset,
                              segEnd - segOffset);
    }

    private void syncRecover(CompletableFuture<ReadDataBuffer> readFuture,
                             String requestId,
                             CmMessage.Copy copy,
                             int start,
                             int readLength,
                             int segIndex,
                             int segOffset,
                             int end,
                             CmMessage.SSLocation ssLocation) {
        new ECSingleSegRangeRecover(requestId, rpcClient, chunkId, ecSchema, copy, segIndex, segOffset, readLength).run().whenComplete((data, t2) -> {
            if (t2 != null) {
                log().error("{} recover chunk {} [{}, {}) failed, location {} {} {}",
                            requestId, chunkIdStr, start, end,
                            SchemaUtils.getDeviceId(ssLocation), SchemaUtils.getPartitionId(ssLocation), ssLocation.getFilename(), t2);
                readFuture.completeExceptionally(new CSException("recover " + requestId + " failed", t2));
                return;
            }
            readFuture.complete(data);
        });
    }
}
