package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

public enum NVMeStatusCode {
    ERROR(0),
    SUCCESS(1);

    private final int code;

    NVMeStatusCode(int code) {
        this.code = code;
    }

    public byte code() {
        return (byte) code;
    }
}
