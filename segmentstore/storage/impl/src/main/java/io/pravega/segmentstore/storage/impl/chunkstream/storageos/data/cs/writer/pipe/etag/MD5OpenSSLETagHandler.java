package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.OpenSSLMD5;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MD5OpenSSLETagHandler extends ETagHandler {
    private static final Logger log = LoggerFactory.getLogger(MD5OpenSSLETagHandler.class);
    private static final Duration md5UpdateDuration = Metrics.makeMetric("MD5OpenSSL.update.duration", Duration.class);

    private final long ctx;

    public MD5OpenSSLETagHandler() {
        super();
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
        if (this.ctx == 0L || !buffer.isNative()) {
            buffer.release();
            return false;
        }
        var start = System.nanoTime();
        var success = OpenSSLMD5.update(ctx, buffer.address() + buffer.position(), buffer.size());
        md5UpdateDuration.updateNanoSecond(System.nanoTime() - start);
        buffer.release();
        return success;
    }

    @Override
    public void end() {
        md5Bytes = OpenSSLMD5.finish(ctx);
        eTagFuture.complete(null);
    }
}
