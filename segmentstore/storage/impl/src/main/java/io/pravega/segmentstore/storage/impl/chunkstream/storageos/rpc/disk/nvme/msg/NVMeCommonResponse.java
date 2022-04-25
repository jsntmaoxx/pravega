package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class NVMeCommonResponse extends NVMeResponse {
    public NVMeCommonResponse(ByteBuffer requestBuffer) {
        super(requestBuffer);
    }

    @Override
    public int waitDataBytes() {
        return 0;
    }

    @Override
    public void feedData(ByteBuf in) {
        throw new UnsupportedOperationException("NVMeCommonResponse is not support feedData");
    }
}
