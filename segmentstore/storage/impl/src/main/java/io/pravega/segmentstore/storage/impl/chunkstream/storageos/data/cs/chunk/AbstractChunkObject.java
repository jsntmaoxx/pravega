package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractChunkObject implements ChunkObject {
    protected final DiskClient<DiskMessage> rpcClient;
    protected final CmClient cmClient;
    protected final CSConfiguration csConfig;
    protected final AtomicInteger dataOffset = new AtomicInteger(0);
    protected final AtomicInteger writingOffset = new AtomicInteger(0);
    protected final UUID chunkId;
    protected final String chunkIdStr;
    protected final AtomicBoolean triggerSealed = new AtomicBoolean(false);
    protected volatile CmMessage.ChunkInfo chunkInfo;

    protected AbstractChunkObject(CmMessage.ChunkInfo chunkInfo, DiskClient<DiskMessage> rpcClient, CmClient cmClient, UUID chunkId, CSConfiguration csConfig) {
        this.chunkInfo = chunkInfo;
        this.rpcClient = rpcClient;
        this.cmClient = cmClient;
        this.chunkId = chunkId;
        this.chunkIdStr = chunkId.toString();
        this.csConfig = csConfig;
    }

    @Override
    public int chunkSize() {
        return chunkInfo.getCapacity();
    }

    @Override
    public void triggerSeal(int length) {
        try {
            cmClient.sealChunk(this, length);
            dataOffset.setOpaque(length);
        } catch (Exception e) {
            log().error("seal chunk {} at {} failed", chunkIdStr, length, e);
        }
    }

    @Override
    public boolean triggerSeal() {
        if (!triggerSealed.compareAndSet(false, true)) {
            return false;
        }
        var sealLength = dataOffset.getOpaque();
        try {
            cmClient.sealChunk(this, sealLength);
        } catch (Exception e) {
            log().error("trigger seal chunk {} at {} failed", chunkIdStr, sealLength, e);
        }
        return true;
    }

    @Override
    public int offset() {
        return dataOffset.getOpaque();
    }

    @Override
    public CmMessage.ChunkInfo chunkInfo() {
        return this.chunkInfo;
    }

    @Override
    public void chunkInfo(CmMessage.ChunkInfo chunkInfo) {
        this.chunkInfo = chunkInfo;
    }

    protected abstract ECSchema ecSchema();

    protected abstract Logger log();

    @Override
    public UUID chunkUUID() {
        return chunkId;
    }

    @Override
    public String chunkIdStr() {
        return chunkIdStr;
    }

    @Override
    public CompletableFuture<Void> writeNativeECSegment(CmMessage.SegmentInfo seg, int codeSegIndex, ByteBuf segByteBuf, int segmentLength, Executor executor) throws Exception {
        var location = seg.getSsLocation();
        var requestId = "nw-ec-" + G.genECRequestId() + "-s" + codeSegIndex;
        var deviceId = SchemaUtils.getDeviceId(location);
        var partitionId = SchemaUtils.getPartitionId(location);
        var bin = location.getFilename();
        var offset = location.getOffset();
        var writeSegSize = CSConfiguration.writeSegmentSize();
        var segPartNum = ecSchema().SegmentLength / writeSegSize + (ecSchema().SegmentLength % writeSegSize > 0 ? 1 : 0);
        if (segPartNum == 1) {
            log().debug("start write native ec {} chunk {}", requestId, chunkIdStr);
            return rpcClient.nativeWrite(deviceId,
                                         requestId,
                                         partitionId,
                                         bin,
                                         offset,
                                         segByteBuf.retain())
                            .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                            .thenAcceptAsync(m -> {
                                if (!m.success()) {
                                    log().error("write native ec segments index {} for chunk {} failed reason {}",
                                                codeSegIndex, chunkIdStr, m.errorMessage());
                                    throw new CSRuntimeException("write ec segment failed");
                                }
                            });
        }

        var writeECSegFutures = new CompletableFuture<?>[segPartNum];
        var dataOffset = 0;
        for (var bi = 0; bi < segPartNum; ++bi) {
            var ecRequestId = requestId + "-p" + bi;
            log().debug("start write native ec {} chunk {}", ecRequestId, chunkIdStr);
            var pbuf = segByteBuf.retainedDuplicate();
            pbuf.readerIndex(dataOffset);
            pbuf.writerIndex(Math.min(dataOffset + writeSegSize, segByteBuf.writerIndex()));
            writeECSegFutures[bi] = rpcClient.nativeWrite(deviceId,
                                                          ecRequestId,
                                                          partitionId,
                                                          bin,
                                                          offset + dataOffset,
                                                          pbuf);
            dataOffset += writeSegSize;
        }
        return CompletableFuture.allOf(writeECSegFutures)
                                .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                                .thenAcceptAsync(m -> {
                                    boolean allSuccess = true;
                                    for (var i = 0; i < writeECSegFutures.length; ++i) {
                                        try {
                                            var response = ((CompletableFuture<DiskMessage>) writeECSegFutures[i]).get();
                                            if (!response.success()) {
                                                log().error("write native ec segments index {} sub {}  for chunk {} request {} failed reason {}",
                                                            codeSegIndex, i, chunkIdStr, response.requestId(), response.errorMessage());
                                            }
                                        } catch (ExecutionException | InterruptedException e) {
                                            log().error("write native ec segments index {} sub {} for chunk {} failed reason {}",
                                                        codeSegIndex, i, chunkIdStr, e);
                                            allSuccess = false;
                                        }
                                    }

                                    if (!allSuccess) {
                                        throw new CSRuntimeException("write ec segment failed");
                                    }
                                }, executor);
    }

    @Override
    public CompletableFuture<Void> writeNativeEC(CmMessage.CopyOrBuilder ecCopy, long codeBufferAddress, int codeNumber, int segmentLength, Executor executor) {
        CompletableFuture<Void> writeECFuture = new CompletableFuture<>();
        if (ecCopy == null) {
            log().error("write native ec failed due to chunk {} has no ec copy {}", chunkIdStr, chunkInfo);
            writeECFuture.completeExceptionally(new CSException("chunk " + chunkIdStr + " has no ec copy"));
            return writeECFuture;
        }

        var writeSegSize = CSConfiguration.writeSegmentSize();
        var segPartNum = ecSchema().SegmentLength / writeSegSize + (ecSchema().SegmentLength % writeSegSize > 0 ? 1 : 0);
        var writeECSegFutures = new CompletableFuture<?>[ecSchema().CodeNumber * segPartNum];
        var schema = ecSchema();
        try {
            var bufs = ecSchema().codeMatrixCache.toByteBufList(codeBufferAddress);
            for (var si = 0; si < schema.CodeNumber; ++si) {
                var segBuf = bufs.get(si).retain();
                var segIndex = si + schema.DataNumber;
                var location = getSegment(ecCopy, segIndex).getSsLocation();
                var requestId = "nw-ec-" + G.genECRequestId() + "-s" + segIndex;
                var deviceId = SchemaUtils.getDeviceId(location);
                var partitionId = SchemaUtils.getPartitionId(location);
                var bin = location.getFilename();
                var offset = location.getOffset();
                if (segPartNum == 1) {
                    log().info("start write native ec {} chunk {}", requestId, chunkIdStr);
                    writeECSegFutures[si] = rpcClient.nativeWrite(deviceId,
                                                                  requestId,
                                                                  partitionId,
                                                                  bin,
                                                                  offset,
                                                                  segBuf);
                } else {
                    var dataOffset = 0;
                    for (var bi = 0; bi < segPartNum; ++bi) {
                        var ecRequestId = requestId + "-p" + bi;
                        log().info("start write native ec {} chunk {}", ecRequestId, chunkIdStr);
                        var pbuf = segBuf.retainedDuplicate();
                        pbuf.readerIndex(dataOffset);
                        pbuf.writerIndex(Math.min(dataOffset + writeSegSize, segBuf.writerIndex()));
                        writeECSegFutures[si * segPartNum + bi] = rpcClient.nativeWrite(deviceId,
                                                                                        ecRequestId,
                                                                                        partitionId,
                                                                                        bin,
                                                                                        offset + dataOffset,
                                                                                        pbuf);
                        dataOffset += writeSegSize;
                    }
                }
            }
        } catch (Exception e) {
            log().error("sendRequest write native ec for chunk {} failed", chunkIdStr, e);
            writeECFuture.completeExceptionally(new CSException("write ec for chunk " + chunkIdStr + " failed"));
            return writeECFuture;
        }
        CompletableFuture.allOf(writeECSegFutures)
                         .orTimeout(csConfig.writeDiskTimeoutSeconds(), TimeUnit.SECONDS)
                         .whenCompleteAsync((r, t) -> {
                             if (t != null) {
                                 log().error("write native ec segments for chunk {} failed", chunkIdStr, t);
                                 writeECFuture.completeExceptionally(t);
                                 return;
                             }
                             writeECFuture.complete(null);
                         }, executor);
        return writeECFuture;
    }

    @Override
    public int writeGranularity() {
        return chunkInfo.getIndexGranularity() + CSConfiguration.writeSegmentOverhead();
    }

    @Override
    public int contentGranularity() {
        return chunkInfo.getIndexGranularity();
    }

    @Override
    public int chunkLength() {
        return chunkInfo.getStatus() == CmMessage.ChunkStatus.SEALED ? chunkInfo.getSealedLength() : dataOffset.getOpaque();
    }

    @Override
    public CompletableFuture<ChunkObject> completeClientEC(int sealLength, CmMessage.Copy ecCopy) throws CSException, IOException {
        return cmClient.completeClientEC(this, sealLength, ecCopy);
    }

    @Override
    public CmMessage.Copy findECCopy() {
        if (chunkInfo.getCopiesCount() <= 0) {
            return null;
        }
        var copy = chunkInfo.getCopies(chunkInfo.getCopiesCount() - 1);
        if (copy.getIsEc()) {
            return copy;
        }
        for (var c : chunkInfo.getCopiesList()) {
            if (c.getIsEc() && c.getIsClientCreatedEcCopy()) {
                return c;
            }
        }
        return null;
    }

    protected CmMessage.SegmentInfo getSegment(CmMessage.CopyOrBuilder copy, int segIndex) throws RuntimeException {
        var seg = findCopySegments(copy, segIndex);
        if (seg != null) {
            return seg;
        }
        log().error("can not find seg {} from chunk {} copy {}", segIndex, chunkIdStr, copy);
        throw new CSRuntimeException("invalid seg " + segIndex + " in chunk " + chunkIdStr);
    }

    @Override
    public CmMessage.SegmentInfo findCopySegments(CmMessage.CopyOrBuilder ecCopy, int ecSegIndex) {
        var seg = ecCopy.getSegments(ecSegIndex);
        if (seg.getSequence() == ecSegIndex) {
            return seg;
        }
        for (var s : ecCopy.getSegmentsList()) {
            // cj_todo need check block status?
            if (s.getSequence() == ecSegIndex) {
                return s;
            }
        }
        return null;
    }

    @Override
    public int writingOffset() {
        return writingOffset.getOpaque();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ChunkObject) {
            return chunkId.equals(((ChunkObject) o).chunkUUID());
        } else if (o instanceof UUID) {
            return (chunkId.getMostSignificantBits() == ((UUID) o).getMostSignificantBits() &&
                    chunkId.getLeastSignificantBits() == ((UUID) o).getLeastSignificantBits());
        }
        return false;
    }

    @Override
    public int compareTo(Object o) {
        if (this == o) {
            return 0;
        }
        if (o instanceof ChunkObject) {
            return chunkId.compareTo(((ChunkObject) o).chunkUUID());
        } else if (o instanceof UUID) {
            return chunkId.compareTo((UUID) o);
        }
        throw new UnsupportedOperationException("ChunkObject can not compare to " + o.getClass().getName());
    }
}
