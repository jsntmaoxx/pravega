package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import jakarta.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyObjectWriter implements ObjectWriter {
    private static final Logger log = LoggerFactory.getLogger(DummyObjectWriter.class);

    @Override
    public boolean onData(WriteDataBuffer buffer) {
        buffer.release();
        return true;
    }

    @Override
    public void onFinishReadData(AsyncContext asyncContext) {
        asyncContext.complete();
    }

    @Override
    public void onErrorReadData(AsyncContext asyncContext, Throwable t) {
        asyncContext.complete();
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public boolean requireExtraFetchData() {
        return true;
    }
}
