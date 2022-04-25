package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Response;
import io.netty.buffer.ByteBuf;

public class HDDCommonResponse extends HDDResponse {

    public HDDCommonResponse(Response response) {
        super(response);
    }

    @Override
    public int waitDataBytes() {
        return 0;
    }

    @Override
    public void feedData(ByteBuf in) {
        throw new UnsupportedOperationException("can not accept data on common disk response");
    }
}
