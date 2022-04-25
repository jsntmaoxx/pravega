package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.SharedMemory;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class ShareMemInfo {
    public static final int ContentSize = ShareMemFileInfo.ContentSize + Integer.BYTES + ShareMemFileInfo.ContentSize + Integer.BYTES;
    public final ShareMemFileInfo ctlFileInfo;
    public final int slotId;
    public final ShareMemFileInfo dataFileInfo;
    public final int dataPageCount;

    public ShareMemInfo(ShareMemFileInfo ctlFileInfo, int slotId, ShareMemFileInfo dataFileInfo, int dataPageCount) {
        this.ctlFileInfo = ctlFileInfo;
        this.slotId = slotId;
        this.dataFileInfo = dataFileInfo;
        this.dataPageCount = dataPageCount;
    }

    public static ShareMemInfo decode(ByteBuf buf) {
        var ctlFile = ShareMemFileInfo.decode(buf);
        var slotId = buf.readIntLE();
        var pageFile = ShareMemFileInfo.decode(buf);
        var pageCount = buf.readIntLE();
        return new ShareMemInfo(ctlFile, slotId, pageFile, pageCount);
    }

    public void encode(ByteBuf buffer) {
        ctlFileInfo.encode(buffer);
        buffer.writeIntLE(slotId);
        dataFileInfo.encode(buffer);
        buffer.writeIntLE(dataPageCount);
    }

    public static class ShareMemFileInfo {
        public static final int MaxFileNameSize = 4096;
        public static final int ContentSize = MaxFileNameSize + 8 + 4;
        public static final byte[] fileNameBuf = new byte[MaxFileNameSize];
        public final long address;
        public final int size;
        public final String fileName;

        public ShareMemFileInfo(long address, int size, String fileName) {
            this.address = address;
            this.size = size;
            this.fileName = fileName;
        }

        public static ShareMemFileInfo decode(ByteBuf buf) {
            var address = buf.readLongLE();
            var size = buf.readIntLE();
            // nvmeengine don't set size
            if (size < SharedMemory.PageSize) {
                size = SharedMemory.PageSize;
            }
            String name;
            synchronized (fileNameBuf) {
                buf.readBytes(fileNameBuf);
                name = StringUtils.bytes2String(fileNameBuf);
            }
            return new ShareMemFileInfo(address, size, name);
        }

        public void encode(ByteBuf buffer) {
            buffer.writeLongLE(this.address);
            buffer.writeIntLE(this.size);
            var nameBuf = fileName.getBytes(StandardCharsets.UTF_8);
            buffer.writeBytes(nameBuf);
            if (nameBuf.length < MaxFileNameSize) {
                buffer.writeZero(MaxFileNameSize - nameBuf.length);
            }
        }
    }
}
