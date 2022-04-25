package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

public class CSException extends Exception {

    private S3ErrorCode error;

    public CSException(String message) {
        super(message);
    }

    public CSException(String message, Throwable t) {
        super(message, t);
    }

    public CSException(String message, S3ErrorCode error) {
        super(message);
        this.error = error;
    }

    public S3ErrorCode getError() {
        return error;
    }
}
