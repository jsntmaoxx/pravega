package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.OpenSSLMD5;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncMD5OpenSSLETagHandler extends ETagHandler {
    private static final Logger log = LoggerFactory.getLogger(AsyncMD5OpenSSLETagHandler.class);
    private static final Duration md5UpdateDuration = Metrics.makeMetric("MD5OpenSSL.async.update.duration", Duration.class);

    private final long ctx;
    private final Executor executor;
    private final Queue<WriteDataBuffer> bufferQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean processing = new AtomicBoolean(false);
    private volatile boolean md5Error = false;
    private volatile boolean allData = false;

    public AsyncMD5OpenSSLETagHandler(Executor executor) {
        super();
        this.executor = executor;
        this.ctx = OpenSSLMD5.init();
        if (this.ctx == 0L) {
            eTagFuture.completeExceptionally(new CSException("init open ssl md5 context failed"));
        }
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        if (md5Error || this.ctx == 0L || !buffer.isNative()) {
            buffer.release();
            return false;
        }
        bufferQueue.offer(buffer);

        if (processing.compareAndSet(false, true)) {
            executor.execute(new UpdateMd5());
        }
        return true;
    }

    @Override
    public void end() {
        if (md5Error) {
            return;
        }
        allData = true;
        if (processing.compareAndSet(false, true)) {
            executor.execute(new UpdateMd5());
        }
    }

    private class UpdateMd5 implements Runnable {
        @Override
        public void run() {
            while (true) {
                var buf = bufferQueue.poll();
                if (buf == null) {
                    if (allData) {
                        md5Bytes = OpenSSLMD5.finish(ctx);
                        eTagFuture.complete(null);
                        break;
                    }
                    if (processing.compareAndSet(true, false)) {
                        if (!bufferQueue.isEmpty() && processing.compareAndSet(false, true)) {
                            continue;
                        }
                        if (allData && processing.compareAndSet(false, true)) {
                            continue;
                        }
                        break;
                    }
                    continue;
                }
                if (md5Error) {
                    buf.release();
                    continue;
                }

                var start = System.nanoTime();
                var success = OpenSSLMD5.update(ctx, buf.address() + buf.position(), buf.size());
                buf.release();
                if (!success) {
                    log().error("async openssl md5 encounter error");
                    md5Error = true;
                    eTagFuture.completeExceptionally(new CSException("async openssl md5 failed"));
                }
                md5UpdateDuration.updateNanoSecond(System.nanoTime() - start);
            }
        }
    }
}
