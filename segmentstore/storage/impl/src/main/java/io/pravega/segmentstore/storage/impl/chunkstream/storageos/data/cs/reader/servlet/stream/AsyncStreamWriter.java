package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.servlet.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Amount;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range.ReadRange;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.WriteListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritePendingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncStreamWriter implements WriteListener {
    private static final Logger log = LoggerFactory.getLogger(AsyncStreamWriter.class);
    private static final Duration read2FlushSocketDuration = Metrics.makeMetric("reader.read.2.flush.socket.duration", Duration.class);
    private static final Count onWritePossibleCount = Metrics.makeMetric("reader.onWritePossible.count", Count.class);
    private static final Duration readChunkDuration = Metrics.makeMetric("reader.read.chunk.duration", Duration.class);
    private static final Amount readChunkAmount = Metrics.makeMetric("reader.read.chunk.size", Amount.class);
    private static final Amount asyncStreamWriteSocketSize = Metrics.makeMetric("reader.write.socket.size", Amount.class);

    private final String objKey;
    private final ReadRange dataRange;
    private final Executor executor;
    private final AsyncContext asyncContext;
    private final CSServletOutputStream outPutStream;
    private final AtomicBoolean reading = new AtomicBoolean(false);
    private final long requestId;
    private final long startTime;
    private final List<ByteBuffer> pendingBuffers = new ArrayList<>();
    private int writeContentSize = 0;

    public AsyncStreamWriter(String objKey, ReadRange dataRange, AsyncContext asyncContext, CSServletOutputStream outPutStream, Executor executor) {
        this.objKey = objKey;
        this.dataRange = dataRange;
        this.executor = executor;
        this.asyncContext = asyncContext;
        this.outPutStream = outPutStream;
        this.requestId = G.genRequestId();
        this.dataRange.requestId(this.requestId);
        this.startTime = System.nanoTime();
    }

    private void fetchData() {
        if (!reading.compareAndSet(false, true)) {
            return;
        }
        try {
            final var startRead = System.nanoTime();
            dataRange.read(executor).whenComplete((buffer, t) -> {
                if (t != null) {
                    log.error("r-{} read obj {} range {} at pos {} failed",
                              requestId, objKey, dataRange, dataRange.readPosition(), t);
                    onError(t);
                    return;
                }
                try {
                    readChunkDuration.updateNanoSecond(System.nanoTime() - startRead);
                    var readChunkSize = buffer.readChunkSize();
                    readChunkAmount.count(readChunkSize);
                    if (log.isDebugEnabled()) {
                        var readPos = dataRange.readPosition();
                        var contentPos = dataRange.contentReadPosition();
                        int contentLength = 0;
                        for (var b : buffer.contentBuffers()) {
                            contentLength += b.remaining();
                        }
                        log.info("r-{} read obj {} chunk [{}, {}) {} content [{}, {}) {} range {} skip {}",
                                 requestId, objKey,
                                 readPos, readPos + readChunkSize, readChunkSize,
                                 contentPos, contentPos + contentLength, contentLength,
                                 dataRange, dataRange.skipReadSize());
                    }
                    dataRange.advanceReadChunkPosition(readChunkSize);
                    appendBuffers(buffer.contentBuffers());
                    reading.set(false);
                    if (!dataRange.readAll() && !outPutStream.isClosed()) {
                        fetchData();
                    }
                    writePendingBuffers();
                } catch (Throwable e) {
                    onError(e);
                }
            });
        } catch (Exception e) {
            log.error("r-{} read obj {} range {} at pos {} failed",
                      requestId, objKey, dataRange, dataRange.readPosition(), e);
            onError(e);
        }
    }

    @Override
    public void onWritePossible() throws IOException {
        if (writeContentSize >= dataRange.contentLength()) {
            if (outPutStream.isReady()) {
                try {
                    outPutStream.flush();
                    assert writeContentSize == dataRange.contentLength();
                    read2FlushSocketDuration.updateNanoSecond(System.nanoTime() - startTime);
                    asyncContext.complete();
                    log.warn("r-{} flush read obj {} in onWritePossible success", requestId, objKey);
                } catch (WritePendingException e) {
                    log.warn("r-{} flush read obj {} in onWritePossible failed, continue flush next time", requestId, objKey);
                }
            } else {
                log.warn("r-{} flush read obj {} in onWritePossible failed due to output is not ready, continue flush next time", requestId, objKey);
            }
            return;
        }
        onWritePossibleCount.increase();
        if (!dataRange.readAll()) {
            fetchData();
        }
        writePendingBuffers();
    }

    @Override
    public void onError(Throwable t) {
        log.error("r-{} read obj {} range {} failed", requestId, objKey, dataRange, t);
        asyncContext.complete();
    }

    private synchronized void appendBuffers(List<ByteBuffer> buffers) {
        for (var b : buffers) {
            skipReadAndAppend(b);
        }
    }

    private void skipReadAndAppend(ByteBuffer buffer) {
        var bufferSize = buffer.limit() - buffer.position();
        var skipReadSize = dataRange.skipReadSize(bufferSize);
        if (skipReadSize <= 0) {
            assert skipReadSize == 0;
            var acceptSize = dataRange.advanceReadContentSize(bufferSize);
            if (acceptSize < bufferSize) {
                buffer.limit(buffer.position() + acceptSize);
            }
            if (buffer.limit() > buffer.position()) {
                pendingBuffers.add(buffer);
            }
            return;
        }

        if (skipReadSize >= bufferSize) {
            // cj_todo remove below code, all buffers are skipped
            //dataRange.advanceReadContentSize(bufferSize);
            return;
        }
        buffer.position(buffer.position() + skipReadSize);
        bufferSize = buffer.limit() - buffer.position();
        var acceptSize = dataRange.advanceReadContentSize(bufferSize);
        if (acceptSize < bufferSize) {
            buffer.limit(buffer.position() + acceptSize);
        }
        if (buffer.limit() > buffer.position()) {
            pendingBuffers.add(buffer);
        }
    }

    private synchronized void writePendingBuffers() throws IOException {
        if (pendingBuffers.isEmpty()) {
            return;
        }

        var writeLen = 0;
        var it = pendingBuffers.iterator();
        while (outPutStream.isReady() && it.hasNext()) {
            var pendBuffer = it.next();
            var len = pendBuffer.remaining();
            outPutStream.write(pendBuffer);
            it.remove();
            writeLen += len;
        }

        if (writeLen > 0) {
            onSuccessWrite(writeLen);
        }
    }

    private void onSuccessWrite(int len) throws IOException {
        asyncStreamWriteSocketSize.count(len);
        writeContentSize += len;
        if (writeContentSize >= dataRange.contentLength()) {
            if (outPutStream.isReady()) {
                try {
                    outPutStream.flush();
                    assert writeContentSize == dataRange.contentLength();
                    read2FlushSocketDuration.updateNanoSecond(System.nanoTime() - startTime);
                    asyncContext.complete();
                } catch (WritePendingException e) {
                    log.warn("r-{} flush read obj {} failed, continue flush in onWritePossible", requestId, objKey);
                }
            } else {
                log.warn("r-{} flush read obj {} failed due to output is not ready, continue flush in onWritePossible", requestId, objKey);
            }
        }
    }
}
