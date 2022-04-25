package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

public class CSRuntimeException extends RuntimeException {
    public CSRuntimeException(String message) {
        super(message);
    }

    public CSRuntimeException(String message, Throwable t) {
        super(message, t);
    }
}
