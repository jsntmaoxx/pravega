package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

public class NVMeShmFileResponse extends NVMeResponse {
    private final ByteBuf contentBuf;

    public NVMeShmFileResponse(ByteBuffer requestBuffer) {
        super(requestBuffer);
        this.contentBuf = Unpooled.buffer(ShareMemInfo.ContentSize, ShareMemInfo.ContentSize);
    }

    @Override
    public int waitDataBytes() {
        return contentBuf.writableBytes();
    }

    @Override
    public void feedData(ByteBuf in) {
        contentBuf.writeBytes(in, Math.min(in.readableBytes(), contentBuf.writableBytes()));
    }

    public ShareMemInfo shareMemInfo() {
        return ShareMemInfo.decode(contentBuf);
    }
}
