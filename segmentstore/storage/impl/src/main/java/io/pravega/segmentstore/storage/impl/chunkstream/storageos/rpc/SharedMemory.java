package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.NativeHugePageSharedMemory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.NativeMemory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.NativePosixSharedMemory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeRpcConfiguration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SharedMemory {
    public static final int PageSize = 2 * 1024 * 1024;
    public static final String nsmPrefix = NVMeRpcConfiguration.enableShareMemByHugePage() ? "/dev/hugepages/cs.sm." : "/dev/shm/cs.sm.";
    public static final String subIndexSplit = "_";
    private static final Logger log = LoggerFactory.getLogger(SharedMemory.class);
    private static final AtomicLong nsmIndex = new AtomicLong(0);

    // for test
    public static void resetShareMemIndex() {
        nsmIndex.set(0);
    }

    public static ShmBufFile allocate() {
        return allocate(PageSize);
    }

    public static ShmBufFile allocate(int size) {
        var index = nsmIndex.getAndIncrement();
        var path = nsmPrefix + index + subIndexSplit + 0;
        var fd = 0;
        var address = 0L;
        try {
            fd = allocatePages(index, 0, size);
            if (fd < 0) {
                return null;
            }
            address = mapPages(fd, size);
            if (address == 0L) {
                return null;
            }
            return new ShmBufFile(path, address, size, index, 0, fd);
        } catch (Exception e) {
            log.error("allocate share mem size {} failed", size, e);
            if (address > 0) {
                unmapPages(address, size);
            }
            if (fd > 0) {
                freePages(index, 0, fd);
            }
            throw e;
        }
    }

    public static List<ShmBufFile> allocate(int size, int count) {
        var bufs = new ArrayList<ShmBufFile>(count);
        var index = nsmIndex.getAndIncrement();
        var fd = 0;
        var address = 0L;
        var subIndex = 0;
        try {
            for (; subIndex < count; ++subIndex) {
                var path = nsmPrefix + index + subIndexSplit + subIndex;
                fd = allocatePages(index, subIndex, size);
                if (fd < 0) {
                    log.error("allocate share mem index {} sub index {} size {} failed", index, subIndex, size);
                    break;
                }
                address = mapPages(fd, size);
                if (address == 0L) {
                    log.error("map share mem index {} sub index {} size {} failed", index, subIndex, size);
                    break;
                }
                bufs.add(new ShmBufFile(path, address, size, index, subIndex, fd));
                fd = 0;
                address = 0L;
            }
            return bufs;
        } catch (Exception e) {
            log.error("allocate share mem {}{}{}{}-{} size {} failed",
                      nsmPrefix, index, subIndexSplit, 0, count, StringUtils.size2String((long) size * count), e);
            throw e;
        } finally {
            if (bufs.size() < count) {
                for (var b : bufs) {
                    free(b);
                }
                if (address > 0) {
                    unmapPages(address, size);
                }
                if (fd > 0) {
                    freePages(index, subIndex, fd);
                }
            }
        }
    }

    public static boolean free(ShmBufFile shmBufFile) {
        unmapPages(shmBufFile.address, shmBufFile.size);
        return freePages(shmBufFile.index, shmBufFile.subIndex, shmBufFile.fd);
    }

    public static boolean free(List<ShmBufFile> shmBufFileList) {
        for (var b : shmBufFileList) {
            free(b);
        }
        shmBufFileList.clear();
        return true;
    }

    // use by client, map shared mem file
    public static ShmBufFile map(String filePath, int size) {
        try {
            var fd = openPages(filePath);
            var address = mapPages(fd, size);
            return new ShmBufFile(filePath, address, size, fd);
        } catch (Exception e) {
            log.error("map {} size {} failed", filePath, size, e);
            throw e;
        }
    }

    // use by client, unmap shared mem file
    public static boolean unmap(ShmBufFile ctlPage) {
        return unmapPages(ctlPage.address, ctlPage.size);
    }

    private static int allocatePages(long index, int subIndex, int size) {
        return NVMeRpcConfiguration.enableShareMemByHugePage()
               ? NativeHugePageSharedMemory.allocate(index, subIndex, size)
               : NativePosixSharedMemory.allocate(index, subIndex, size);
    }

    private static int openPages(String filePath) {
        return NVMeRpcConfiguration.enableShareMemByHugePage()
               ? NativeHugePageSharedMemory.open(filePath)
               : NativePosixSharedMemory.open(filePath);
    }

    private static long mapPages(int fd, int size) {
        return NVMeRpcConfiguration.enableShareMemByHugePage()
               ? NativeHugePageSharedMemory.map(fd, size)
               : NativePosixSharedMemory.map(fd, size);
    }

    private static boolean unmapPages(long address, int size) {
        return NVMeRpcConfiguration.enableShareMemByHugePage()
               ? NativeHugePageSharedMemory.unmap(address, size)
               : NativePosixSharedMemory.unmap(address, size);
    }

    private static boolean freePages(long index, int subIndex, int fd) {
        return NVMeRpcConfiguration.enableShareMemByHugePage()
               ? NativeHugePageSharedMemory.free(fd, index, subIndex)
               : NativePosixSharedMemory.free(fd, index, subIndex);
    }

    public static class ShmBufFile {
        public final String path;
        public final long address;
        public final int size;
        public final long index;
        public final int subIndex;
        public final ByteBuf byteBuf;
        public final ByteBuffer byteBuffer;
        private final int fd;

        ShmBufFile(String path, long address, int size, long index, int subIndex, int fd) {
            this.path = path;
            this.address = address;
            this.size = size;
            this.index = index;
            this.subIndex = subIndex;
            this.fd = fd;
            this.byteBuffer = NativeMemory.directBuffer(this.address, this.size);
            this.byteBuf = Unpooled.wrappedBuffer(byteBuffer.duplicate());
        }

        ShmBufFile(String path, long address, int size, int fd) {
            this.path = path;
            this.address = address;
            this.size = size;
            this.fd = fd;
            this.index = -1;
            this.subIndex = -1;
            this.byteBuffer = NativeMemory.directBuffer(this.address, this.size);
            this.byteBuf = Unpooled.wrappedBuffer(byteBuffer.duplicate());
        }

        public ByteBuffer toDirectBuffer() {
            return NativeMemory.directBuffer(address, size);
        }
    }
}
