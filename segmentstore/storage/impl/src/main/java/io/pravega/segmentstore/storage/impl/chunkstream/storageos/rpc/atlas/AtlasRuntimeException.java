package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Exception to wrap the status of a shaded io.grpc.StatusRuntimeException
 */
public class AtlasRuntimeException extends RuntimeException {
    private Status status;

    public AtlasRuntimeException(StatusRuntimeException e) {
        super(e.getMessage(), e.getCause());
        status = e.getStatus();
    }

    public AtlasRuntimeException(Status status) {
        super(status.toString());
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
