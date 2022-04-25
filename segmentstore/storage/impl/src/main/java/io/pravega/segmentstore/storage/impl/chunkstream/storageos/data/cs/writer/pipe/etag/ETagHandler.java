package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.DefaultExceptionHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.NamedForkJoinWorkerThreadFactory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.ForkJoinPoolStats;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.InputDataBufferHandler;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

public abstract class ETagHandler implements InputDataBufferHandler {
    private static final Logger log = LoggerFactory.getLogger(ETagHandler.class);
    protected static ForkJoinPool executor;
    protected final CompletableFuture<Void> eTagFuture = new CompletableFuture<>();
    protected volatile byte[] md5Bytes;

    public static ETagHandler createETagHandler(CSConfiguration csConfig, boolean isSharedObject) {
        if (csConfig.skipMD5()) {
            return new DummyETagHandler();
        }
        if (isSharedObject) {
            return new MD5JDKETagHandler();
        }
        if (executor == null) {
            executor = new ForkJoinPool(csConfig.eTagThreadNumber(),
                                        new NamedForkJoinWorkerThreadFactory("ec-"),
                                        new DefaultExceptionHandler(log),
                                        true);
            Metrics.makeStatsMetric("forkjoin.pool.etag", new ForkJoinPoolStats(executor));
        }
        return csConfig.useNativeBuffer() ? new AsyncMD5OpenSSLETagHandler(executor) : new MD5JDKETagHandler();
    }

    @Override
    public void errorEnd() {
        if (!eTagFuture.isDone()) {
            log().warn("cancel ETag task");
            eTagFuture.completeExceptionally(new CSException("compute eTag is canceled"));
        }
    }

    @Override
    public void cancel() {
        eTagFuture.cancel(true);
    }

    protected abstract Logger log();

    public byte[] md5Bytes() {
        assert eTagFuture.isDone();
        return md5Bytes;
    }

    public String md5Base64() {
        assert eTagFuture.isDone();
        return md5Bytes != null ? Base64.getEncoder().encodeToString(md5Bytes) : null;
    }

    public String eTagHex() {
        assert eTagFuture.isDone();
        return md5Bytes != null ? Hex.encodeHexString(md5Bytes) : null;
    }

    public CompletableFuture<Void> eTagFuture() {
        return eTagFuture;
    }
}
