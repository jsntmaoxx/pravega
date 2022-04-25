package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Response;
import io.netty.buffer.ByteBuf;

public class HDDReadResponse extends HDDResponse implements ReadResponse {
    private final ReadDataBuffer readBuffer;

    public HDDReadResponse(Response response, int dataLength) {
        super(response);
        this.readBuffer = ReadDataBuffer.create(dataLength, true);
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
