package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CSServletInputStream {
    boolean isReady();

    int read(ByteBuffer byteBuffer) throws IOException;

    boolean isFinished();
}
