package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.SharedMemory;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicBoolean;

public class NVMeShareMemBuffer {
    public static final int NVMeMessageSize = 4 * 1024;
    private static final int ResponseFlagOffset = 64 * 1024;
    private final SharedMemory.ShmBufFile ctlPage;
    private final int flagOffset;
    private final long clearMask;
    private final long valueMask;
    private final int index;
    private final SharedMemory.ShmBufFile dataPage1;
    private final SharedMemory.ShmBufFile dataPage2;
    private final AtomicBoolean free;

    public NVMeShareMemBuffer(SharedMemory.ShmBufFile ctlPage,
                              int flagOffset,
                              int index,
                              SharedMemory.ShmBufFile dataPage1,
                              SharedMemory.ShmBufFile dataPage2) {
        this.ctlPage = ctlPage;
        this.flagOffset = flagOffset;
        this.clearMask = ~(0xffL << index * Byte.SIZE);
        this.valueMask = 0x01L << index * Byte.SIZE;
        this.index = index;
        this.dataPage1 = dataPage1;
        this.dataPage2 = dataPage2;
        unlockRequestFlag();
        unlockResponseFlag();
        this.free = new AtomicBoolean(true);
    }

    public static long requestFlag(SharedMemory.ShmBufFile ctlPage, int requestFlagOffset) {
        return G.U.getLongVolatile(null, ctlPage.address + requestFlagOffset);
    }

    public static long responseFlag(SharedMemory.ShmBufFile ctlPage, int requestFlagOffset) {
        return G.U.getLongVolatile(null, ctlPage.address + requestFlagOffset + ResponseFlagOffset);
    }

    public boolean hasRequestFlag() {
        return (G.U.getLongVolatile(null, ctlPage.address + flagOffset) & valueMask) != 0L;
    }

    public boolean hasResponseFlag() {
        return (G.U.getLongVolatile(null, ctlPage.address + flagOffset + ResponseFlagOffset) & valueMask) != 0L;
    }

    // jdk.internal.misc.Unsafe support cms on byte but it needs add
    // --add-opens java.base/jdk.internal.misc=ALL-UNNAMED on jvm
    // --add-exports java.base/jdk.internal.misc=ALL-UNNAMED on compile
    // support it when required
    public boolean tryLockRequestFlag() {
        var flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset);
        var nFlag = (flag & clearMask) | valueMask;
        return G.U.compareAndSwapLong(null, ctlPage.address + flagOffset, flag, nFlag);
    }

    public boolean tryLockResponseFlag() {
        var flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset + ResponseFlagOffset);
        var nFlag = (flag & clearMask) | valueMask;
        return G.U.compareAndSwapLong(null, ctlPage.address + flagOffset + ResponseFlagOffset, flag, nFlag);
    }

    public void lockRequestFlag() {
        long flag, nFlag;
        do {
            flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset);
            nFlag = (flag & clearMask) | valueMask;
        } while (!G.U.compareAndSwapLong(null, ctlPage.address + flagOffset, flag, nFlag));
    }

    public void lockResponseFlag() {
        long flag, nFlag;
        do {
            flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset + ResponseFlagOffset);
            nFlag = (flag & clearMask) | valueMask;
        } while (!G.U.compareAndSwapLong(null, ctlPage.address + flagOffset + ResponseFlagOffset, flag, nFlag));
    }

    public boolean processing(long processingFlag) {
        return (processingFlag & valueMask) != 0L;
    }

    public boolean request(long requestFlag) {
        return (requestFlag & valueMask) != 0;
    }

    public void unlockRequestFlag() {
        long flag, nFlag;
        do {
            flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset);
            nFlag = flag & clearMask;
        } while (!G.U.compareAndSwapLong(null, ctlPage.address + flagOffset, flag, nFlag));
    }

    public boolean tryLockResponseFlag(long requestFlag) {
        var nFlag = (requestFlag & clearMask) | valueMask;
        return G.U.compareAndSwapLong(null, ctlPage.address + flagOffset + ResponseFlagOffset, requestFlag, nFlag);
    }

    public boolean response(long responseFlag) {
        return (responseFlag & valueMask) != 0;
    }

    public void unlockResponseFlag() {
        long flag, nFlag;
        do {
            flag = G.U.getLongVolatile(null, ctlPage.address + flagOffset + ResponseFlagOffset);
            nFlag = flag & clearMask;
        } while (!G.U.compareAndSwapLong(null, ctlPage.address + flagOffset + ResponseFlagOffset, flag, nFlag));
    }

    public SharedMemory.ShmBufFile ctlPage() {
        return ctlPage;
    }

    public SharedMemory.ShmBufFile dataPage1() {
        return dataPage1;
    }

    public SharedMemory.ShmBufFile dataPage2() {
        return dataPage2;
    }

    public void write(ByteBuf buf) throws CSException {
        if (buf.readableBytes() > dataPage1.byteBuf.writableBytes() + dataPage2.byteBuf.writableBytes()) {
            throw new CSException("data size " + buf.readableBytes() + " is larger than share mem buf size " + (dataPage1.size + dataPage2.size));
        }
        dataPage1.byteBuf.writeBytes(buf, Math.min(dataPage1.byteBuf.writableBytes(), buf.readableBytes()));
        if (buf.readableBytes() > 0) {
            dataPage2.byteBuf.writeBytes(buf, Math.min(dataPage2.byteBuf.writableBytes(), buf.readableBytes()));
        }
    }

    public int index() {
        return index;
    }

    public void resetForWrite() {
        dataPage1.byteBuf.clear();
        dataPage1.byteBuffer.clear();
        dataPage2.byteBuf.clear();
        dataPage2.byteBuffer.clear();
    }

    public void resetForRead() {
        dataPage1.byteBuf.readerIndex(0);
        dataPage1.byteBuf.writerIndex(dataPage1.byteBuf.capacity());
        dataPage1.byteBuffer.clear();

        dataPage2.byteBuf.readerIndex(0);
        dataPage2.byteBuf.writerIndex(dataPage2.byteBuf.capacity());
        dataPage2.byteBuffer.clear();
    }

    public boolean lock() {
        return free.compareAndSet(true, false);
    }

    public void unlock() {
        free.set(true);
    }

    public long clearMask() {
        return clearMask;
    }

    public long valueMask() {
        return valueMask;
    }
}
