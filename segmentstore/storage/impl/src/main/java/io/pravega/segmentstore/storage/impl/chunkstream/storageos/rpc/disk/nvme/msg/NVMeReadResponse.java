package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class NVMeReadResponse extends NVMeResponse implements ReadResponse {
    private final ReadDataBuffer readBuffer;

    public NVMeReadResponse(ByteBuffer requestBuffer) {
        super(requestBuffer);
        this.readBuffer = ReadDataBuffer.create(dataSize(), true);
    }

    @Override
    public int waitDataBytes() {
        return readBuffer.writableBytes();
    }

    @Override
    public void feedData(ByteBuf in) {
        readBuffer.put(in);
    }

    @Override
    public ReadDataBuffer data() {
        return readBuffer;
    }
}
