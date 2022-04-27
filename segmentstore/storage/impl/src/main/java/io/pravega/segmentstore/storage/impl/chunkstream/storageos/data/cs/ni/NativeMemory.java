package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

import java.nio.ByteBuffer;

public class NativeMemory {
    public static native long allocate(int size);

    public static native long allocate(int size, short alignment);

    public static native void free(long address);

    public static native long numa_allocate(int size, int numa_node);

    public static native void numa_free(long address, int size);

    public static native ByteBuffer directBuffer(long address, int size);
}
