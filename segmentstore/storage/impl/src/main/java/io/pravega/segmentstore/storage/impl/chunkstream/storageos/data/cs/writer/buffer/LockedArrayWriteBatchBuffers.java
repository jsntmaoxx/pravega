package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LockedArrayWriteBatchBuffers implements WriteBatchBuffers {
    private static final Logger log = LoggerFactory.getLogger(ArrayWriteBatchBuffers.class);
    private static final Duration WriteFirstDuration = Metrics.makeMetric("LockedArrayBatchBuffer.write.first.duration", Duration.class);
    private static final Duration DataBufferWriteDuration = Metrics.makeMetric("LockedArrayBatchBuffer.dataBuffer.write.duration", Duration.class);

    private final List<WriteDataBuffer> buffers = new ArrayList<>(16);
    private final long batchRequestId;
    private volatile int size = 0;
    private volatile boolean seal = false;

    public LockedArrayWriteBatchBuffers() {
        batchRequestId = G.genBatchRequestId();
    }

    @Override
    public boolean add(WriteDataBuffer buffer) {
        var len = buffer.size();
        assert len > 0 && len < CSConfiguration.writeSegmentSize();

        synchronized (this) {
            if (seal) {
                return false;
            }
            if (size + len <= CSConfiguration.writeSegmentSize()) {
                buffers.add(buffer);
                size += len;
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<WriteDataBuffer> buffers() {
        if (!seal) {
            throw new CSRuntimeException("Get buffers from unsealed LockedArrayBatchBuffers");
        }
        return buffers;
    }

    @Override
    public void completeWithException(Throwable t) {
        if (!seal) {
            throw new CSRuntimeException("Get buffers from unsealed LockedArrayBatchBuffers");
        }
        for (var b : buffers) {
            b.markCompleteWithException(t);
        }
    }

    @Override
    public void complete(Location location, ChunkObject chunkObj) {
        if (!seal) {
            throw new CSRuntimeException("Get buffers from unsealed LockedArrayBatchBuffers");
        }
        var endWrite = System.nanoTime();
        WriteFirstDuration.updateNanoSecond(endWrite - firstDataBufferCreateNano());
        var start = location.offset;
        for (var b : buffers) {
            var end = start + b.size();
            if (end > location.endOffset()) {
                log.error("Write chunk {} [{},{}){}, but sub {} buffer is in scope [{},{}){}",
                          location.chunkId.toString(), location.offset, location.endOffset(), location.length,
                          b, start, end, b.size());
                b.markCompleteWithException(new CSException("unmatched write scope"));
                start += b.size();
                continue;
            }
            DataBufferWriteDuration.updateNanoSecond(endWrite - b.createNano());
            if (!b.markComplete(new Location(location.chunkId, start, end - start), chunkObj)) {
                log.error("Complete chunk {} [{},{}){}, buffer {} failed",
                          location.chunkId.toString(), location.offset, location.endOffset(), location.length, b);
            }
            start += b.size();
        }
    }

    @Override
    public String requestId() {
        return "lab" + batchRequestId;
    }

    @Override
    public void seal() {
        synchronized (this) {
            seal = true;
        }
    }

    @Override
    public WriteDataBuffer getBuffer(int i) {
        return buffers.get(i);
    }

    @Override
    public long firstDataBufferCreateNano() {
        assert !buffers.isEmpty();
        return buffers.get(0).createNano();
    }

    @Override
    public WriteBatchBuffers[] split(int position) {
        throw new UnsupportedOperationException("LockedArrayWriteBatchBuffers is not support split");
    }

    @Override
    public void updateEpoch(byte[] epoch) {
        for (var b : buffers) {
            b.updateFooterEpoch(epoch);
        }
    }

    @Override
    public String toString() {
        if (!seal) {
            throw new CSRuntimeException("Get buffers from unsealed LockedArrayBatchBuffers");
        }
        var now = System.nanoTime();
        StringBuilder s = new StringBuilder(buffers.size() * 16 + 16);
        s.append("lab").append(batchRequestId).append("-").append(buffers.size()).append("-").append(size).append("[");
        for (var b : buffers) {
            s.append(b).append("/").append((now - b.createNano()) / 1000).append(",");
        }
        s.deleteCharAt(s.length() - 1);
        s.append("]");
        return s.toString();
    }
}