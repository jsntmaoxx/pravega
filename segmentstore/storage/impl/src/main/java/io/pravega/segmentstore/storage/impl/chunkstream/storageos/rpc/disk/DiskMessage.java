package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk;

public interface DiskMessage {
    String requestId();

    long requestCreateNano();

    long responseReceiveNano();

    boolean success();

    String errorMessage();
}
