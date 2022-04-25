package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.recover;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.SchemaUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Amount;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractECRecover implements Recover {
    private static final Duration recoverReadSegmentDuration = Metrics.makeMetric("ECRecover.r.seg.duration", Duration.class);
    private static final Amount recoverReadSegmentBW = Metrics.makeMetric("ECRecover.r.seg.bw", Amount.class);

    protected final String requestId;
    protected final DiskClient<? extends DiskMessage> diskClient;
    protected final UUID chunkId;
    protected final ECSchema ecSchema;
    protected final CmMessage.Copy copy;

    public AbstractECRecover(String requestId, DiskClient<? extends DiskMessage> diskClient, UUID chunkId, ECSchema ecSchema, CmMessage.Copy copy) {
        this.requestId = requestId;
        this.diskClient = diskClient;
        this.chunkId = chunkId;
        this.ecSchema = ecSchema;
        this.copy = copy;
    }

    public abstract CompletableFuture<ReadDataBuffer> run();

    protected DataSet prepareDataSet(int segOffset, int length, List<Integer> vExcludeSegIndex) {
        var dataSet = new DataSet(ecSchema);
        for (var i = 0; i < copy.getSegmentsCount(); ++i) {
            var s = copy.getSegments(i);
            var seq = s.getSequence();
            if (vExcludeSegIndex.contains(s.getSequence())) {
                dataSet.vReadSeg[seq] = null;
                dataSet.segState[seq] = false;
                continue;
            }
            var loc = s.getSsLocation();
            if (loc.hasRecoveryStatus()) {
                dataSet.vReadSeg[seq] = null;
                dataSet.segState[seq] = false;
                continue;
            }
            if (dataSet.vReadSeg[seq] != null && dataSet.segState[seq]) {
                // skip unhealthy block if we have duplicate seg
                if (loc.getStatus() != CmMessage.BlockStatus.HEALTHY) {
                    continue;
                }
                log().warn("recovery find duplicate segment in chunk {} segment seq {} copy {}", chunkId, seq, copy);
            }
            dataSet.vReadSeg[seq] = new ReadSeg(SchemaUtils.getDeviceId(loc),
                                                SchemaUtils.getPartitionId(loc),
                                                loc.getFilename(),
                                                loc.getOffset() + segOffset,
                                                length,
                                                loc.getStatus());
            dataSet.segState[seq] = loc.getStatus() == CmMessage.BlockStatus.HEALTHY;
        }
        return dataSet;
    }

    protected void readAndRecover(CompletableFuture<ReadDataBuffer> recoverFuture, DataSet dataSet, int targetReadCount) {
        readData(dataSet, targetReadCount)
                .whenComplete((allSuccess, t) -> {
                    if (t != null) {
                        log().error("{} recover chunk {} read data failed", requestId, chunkId, t);
                        recoverFuture.completeExceptionally(t);
                        return;
                    }
                    if (!allSuccess) {
                        var rCount = 0;
                        for (var rs : dataSet.vReadSeg) {
                            if (rs != null && rs.dataBuffer != null) {
                                ++rCount;
                            }
                        }
                        if (targetReadCount > rCount) {
                            readAndRecover(recoverFuture, dataSet, targetReadCount - rCount);
                            return;
                        }
                        log().warn("{} recover chunk {} read enough data, but still report failed to read all segment",
                                   requestId, chunkId);
                    }
                    recover(recoverFuture, dataSet);
                });
    }

    protected abstract void recover(CompletableFuture<ReadDataBuffer> recoverFuture, DataSet dataSet);

    protected ReadSeg[] selectReadSegment(DataSet dataSet, int targetDataNumber) {
        var vReadSeg = new ReadSeg[targetDataNumber];
        var leftDataNumber = targetDataNumber;
        for (var w = ReadSeg.MaxReadWeight; w >= ReadSeg.MinReadWeight; --w) {
            for (var rs : dataSet.vReadSeg) {
                if (rs == null || rs.readWeight != w) {
                    continue;
                }
                vReadSeg[leftDataNumber - 1] = rs;
                rs.readWeight = ReadSeg.SelectedReadWeight;
                if (--leftDataNumber < 1) {
                    break;
                }
            }
        }
        if (leftDataNumber > 0) {
            log().error("recover chunk {} failed due to can not find enough segment to read, require {} gap {}",
                        chunkId, targetDataNumber, leftDataNumber);
            return null;
        }
        return vReadSeg;
    }

    protected CompletableFuture<Boolean> readData(DataSet dataSet, int targetDataNumber) {
        var allFuture = new CompletableFuture<Boolean>();
        var vReadSeg = selectReadSegment(dataSet, targetDataNumber);
        if (vReadSeg == null) {
            allFuture.completeExceptionally(new CSException("recover can not find enough segment to read"));
            return allFuture;
        }
        var allSuccess = new AtomicBoolean(true);
        var count = new AtomicInteger(vReadSeg.length);
        for (var i = 0; i < vReadSeg.length; ++i) {
            var subR = requestId + "-rc" + i;
            var rs = vReadSeg[i];
            try {
                var startNano = System.nanoTime();
                diskClient.read(rs.device, subR, rs.disk, rs.bin, rs.offset, rs.size)
                          .whenComplete((response, t) -> {
                              if (t != null) {
                                  log().error("{} recovery read chunk {} location {} {} [{},{}]{} complete with error",
                                              subR, chunkId, rs.disk, rs.bin, rs.offset, rs.offset + rs.size, rs.size, t);
                                  allSuccess.setOpaque(false);
                                  if (count.decrementAndGet() == 0) {
                                      allFuture.complete(allSuccess.getOpaque());
                                  }
                              }
                              if (!response.success()) {
                                  log().error("{} recovery read chunk {} location {} {} [{},{}]{} response failed, error {}",
                                              subR, chunkId, rs.disk, rs.bin, rs.offset, rs.offset + rs.size, rs.size, response.errorMessage());
                                  allSuccess.setOpaque(false);
                                  if (count.decrementAndGet() == 0) {
                                      allFuture.complete(allSuccess.getOpaque());
                                  }
                                  return;
                              }
                              recoverReadSegmentDuration.updateNanoSecond(System.nanoTime() - startNano);
                              recoverReadSegmentBW.count(rs.size);

                              rs.dataBuffer = ((ReadResponse) response).data();
                              if (count.decrementAndGet() == 0) {
                                  allFuture.complete(allSuccess.getOpaque());
                              }
                          });
                rs.readWeight = ReadSeg.SelectedReadWeight;
            } catch (Exception e) {
                log().error("skip read {} chunk {} location {} {} [{},{}]{}",
                            subR, chunkId, rs.disk, rs.bin, rs.offset, rs.offset + rs.size, rs.size, e);
                allSuccess.setOpaque(false);
                if (count.decrementAndGet() == 0) {
                    allFuture.complete(allSuccess.getOpaque());
                }
            }
        }
        return allFuture;
    }

    protected abstract Logger log();

    protected static class ReadSeg {
        static final int SelectedReadWeight = Integer.MAX_VALUE;
        static final int MaxReadWeight = 3;
        static final int MinReadWeight = 0;
        final String device;
        final String disk;
        final String bin;
        final long offset;
        final int size;
        int readWeight;
        ReadDataBuffer dataBuffer;

        public ReadSeg(String device, String disk, String bin, long offset, int size, CmMessage.BlockStatus status) {
            this.device = device;
            this.disk = disk;
            this.bin = bin;
            this.offset = offset;
            this.size = size;
            this.readWeight = Math.max(0, MaxReadWeight - status.getNumber());
        }
    }

    protected static class DataSet {
        final ReadSeg[] vReadSeg;
        final boolean[] segState;

        DataSet(ECSchema ecSchema) {
            this.vReadSeg = new ReadSeg[ecSchema.DataNumber + ecSchema.CodeNumber];
            this.segState = new boolean[ecSchema.DataNumber + ecSchema.CodeNumber];
        }
    }
}
