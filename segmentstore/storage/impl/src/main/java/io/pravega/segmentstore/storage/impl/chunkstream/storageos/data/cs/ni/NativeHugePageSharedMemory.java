package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

public class NativeHugePageSharedMemory {
    public static native int allocate(long index, int size);

    public static native int allocate(long index, int subIndex, int size);

    public static native boolean free(int fd, long index, int subIndex);

    public static native int open(String filePath);

    public static native long map(int fd, int size);

    public static native boolean unmap(long address, int size);
}
