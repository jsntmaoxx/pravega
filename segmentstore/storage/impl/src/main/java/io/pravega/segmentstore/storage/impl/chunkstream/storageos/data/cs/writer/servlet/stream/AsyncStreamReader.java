package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Amount;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ReadListener;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncStreamReader implements ReadListener {
    private static final Duration asyncStreamReadSocketDuration = Metrics.makeMetric("asyncStream.readSocket.duration", Duration.class);
    private static final Amount asyncStreamReadSocketSize = Metrics.makeMetric("asyncStream.readSocket.size", Amount.class);
    protected final String objKey;
    protected final CSServletInputStream inputStream;
    protected final int indexGranularity;
    protected final AsyncContext asyncContext;
    protected final CSConfiguration csConfig;
    protected final AtomicBoolean requireData = new AtomicBoolean(true);
    protected final long requestId;
    protected final long contentLength;
    protected long leftLength;
    private WriteDataBuffer buffer;

    public AsyncStreamReader(String objKey,
                             CSServletInputStream inputStream,
                             long contentLength,
                             int indexGranularity,
                             AsyncContext asyncContext,
                             CSConfiguration csConfig) {
        this.objKey = objKey;
        this.inputStream = inputStream;
        this.contentLength = this.leftLength = contentLength;
        this.indexGranularity = indexGranularity;
        this.asyncContext = asyncContext;
        this.csConfig = csConfig;
        this.requestId = G.genRequestId();
    }

    protected void read() {
        try {
            while (requireData.getAcquire()) {
                if (!inputStream.isReady()) {
                    // The jetty thread may trigger onDataAvailable()->read() immediately after isReady() == false.
                    log().trace("r-{} obj {} wait data", requestId, objKey);
                    return;
                }
                if (buffer == null) {
                    buffer = makeBuffer(contentLength - leftLength);
                }
                var startRead = System.nanoTime();
                var readLen = inputStream.read(buffer.shallowByteBuffer());
                if (readLen == -1) {
                    log().trace("r-{} obj {} exit read data", requestId, objKey);
                    break;
                }

                if (buffer == StringUtils.dumbBuffer) {
                    log().error("r-{} obj {} dumbBuffer consume {} byte data", requestId, objKey, readLen);
                    continue;
                }
                asyncStreamReadSocketDuration.updateNanoSecond(System.nanoTime() - startRead);
                asyncStreamReadSocketSize.count(readLen);
                buffer.advancePosition(readLen);
                leftLength -= readLen;
                if (!buffer.hasRemaining() || leftLength <= 0) {
                    var b = buffer;
                    buffer = null;
                    b.truncateAtCurrentDataPosition().updateHeader().updateFooterChecksum().reset();
                    var continueRead = offerData(b);
                    if (!continueRead) {
                        log().debug("r-{} obj {} stop read data", requestId, objKey);
                        if (requireData.compareAndSet(true, false)) {
                            if (requireExtraFetchData()) {
                                if (requireData.compareAndSet(false, true)) {
                                    log().info("r-{} obj {} continue read data after check again", requestId, objKey);
                                    continue;
                                }
                            }
                            // must break, other thread may change value of requireData
                            // do not test requireData again and continue run in out while loop
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            onError(e);
        }
    }

    protected abstract boolean offerData(WriteDataBuffer buffer) throws CSException, InterruptedException;

    protected abstract WriteDataBuffer makeBuffer(long objStart);

    protected abstract boolean requireExtraFetchData();

    protected abstract Logger log();
}
