package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyETagHandler extends ETagHandler {
    private static final Logger log = LoggerFactory.getLogger(DummyETagHandler.class);
    private static final byte[] dummyMd5Bytes = new byte[]{-109, 0, 83, -26, -44, 92, -109, -1, 38, 102, -55, -117, -51, -106, 16, -95};

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        buffer.release();
        return true;
    }

    @Override
    public void end() {
        md5Bytes = dummyMd5Bytes;
        eTagFuture.complete(null);
    }

    @Override
    protected Logger log() {
        return log;
    }
}
