package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ArrayWriteBatchBuffers implements WriteBatchBuffers {
    private static final Logger log = LoggerFactory.getLogger(ArrayWriteBatchBuffers.class);
    private static final Duration DataBufferWriteDuration = Metrics.makeMetric("ArrayBatchBuffer.dataBuffer.write.duration", Duration.class);

    private final List<WriteDataBuffer> buffers = new ArrayList<>(8);
    private final long batchRequestId;
    private int size = 0;
    private boolean seal = false;

    public ArrayWriteBatchBuffers() {
        this.batchRequestId = G.genBatchRequestId();
    }

    /*
    ArrayWriteBatchBuffers used on type2 path, so only single thread add a buffer
    But the seal has a race condition between write thread and fetch data thread
     */
    @Override
    public boolean add(WriteDataBuffer buffer) {
        var len = buffer.size();
        assert len > 0 && len <= CSConfiguration.writeSegmentSize();

        if (size + len <= CSConfiguration.writeSegmentSize()) {
            synchronized (this) {
                if (seal) {
                    return false;
                }
                buffers.add(buffer);
                size += len;
            }
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<WriteDataBuffer> buffers() {
        return buffers;
    }

    @Override
    public void completeWithException(Throwable t) {
        for (var b : buffers) {
            b.markCompleteWithException(t);
        }
    }

    @Override
    public void complete(Location location, ChunkObject chunkObj) {
        var endWrite = System.nanoTime();
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
        return "ab" + batchRequestId;
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
    public WriteBatchBuffers[] split(int splitPos) throws CSException {
        if (splitPos >= size) {
            log.error("split buffer {} at position {} failed", this, splitPos);
            throw new CSException("split position less than buffer size");
        }
        var hintCapacity = buffers.size() / 2 + 1;
        var p1 = new SplitArrayWriteBatchBuffers(this, 0, hintCapacity);
        var p2 = new SplitArrayWriteBatchBuffers(this, 1, hintCapacity);
        var pSize = 0;
        for (var b : buffers) {
            if (pSize + b.size() <= splitPos) {
                p1.add(b.duplicate());
                pSize += b.size();
            } else if (pSize >= splitPos) {
                p2.add(b.duplicate());
            } else {
                var sp = splitPos - pSize;
                assert sp < b.size();
                var s1 = b.duplicate();
                s1.limit(s1.position() + sp);
                p1.add(s1);
                pSize += sp;
                var s2 = b.duplicate();
                s2.advancePosition(sp);
                p2.add(s2);
            }
        }
        return new WriteBatchBuffers[]{p1, p2};
    }

    @Override
    public void updateEpoch(byte[] epoch) {
        for (var b : buffers) {
            b.updateFooterEpoch(epoch);
        }
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder(buffers.size() * 16 + 16);
        s.append("ab").append(batchRequestId).append("-").append(buffers.size()).append("-").append(size).append("[");
        for (var b : buffers) {
            s.append(b).append(",");
        }
        s.deleteCharAt(s.length() - 1);
        s.append("]");
        return s.toString();
    }
}
