package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.servlet.stream;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CSServletOutputStream {
    boolean isReady();

    boolean isClosed();

    void flush() throws IOException;

    void write(ByteBuffer byteBuffer) throws IOException;
}
