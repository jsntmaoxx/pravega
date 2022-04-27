package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

public class OpenSSLMD5 {
    public static native long init();

    public static native boolean update(long ctx, long buffer, long length);

    public static native byte[] finish(long ctx);
}
