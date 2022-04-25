package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5JDKETagHandler extends ETagHandler {
    private static final Logger log = LoggerFactory.getLogger(MD5JDKETagHandler.class);
    private static final Duration md5UpdateDuration = Metrics.makeMetric("MD5JDK.update.duration", Duration.class);
    private final MessageDigest md;
    protected long contentLen;

    public MD5JDKETagHandler() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed get MD5 digest", e);
            eTagFuture.completeExceptionally(e);
        }
        this.md = md;
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        var len = buffer.size();
        contentLen += len;
        var start = System.nanoTime();
        md.update(buffer.array(), buffer.position(), len);
        md5UpdateDuration.updateNanoSecond(System.nanoTime() - start);
        return true;
    }

    @Override
    public void end() {
        md5Bytes = md.digest();
        eTagFuture.complete(null);
        log.trace("ETag algorithm {} contentLen {} base64 {}", md.getAlgorithm(), contentLen, md5Base64());
    }
}
