package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import jakarta.servlet.AsyncContext;
import org.slf4j.Logger;

public interface ObjectWriter {
    /*
    onData will accept the buffer, or it will throw a CSException
    return true means still require more data
     */
    boolean onData(WriteDataBuffer buffer) throws CSException;

    void onFinishReadData(AsyncContext asyncContext);

    void onErrorReadData(AsyncContext asyncContext, Throwable t);

    Logger log();

    boolean requireExtraFetchData();
}
