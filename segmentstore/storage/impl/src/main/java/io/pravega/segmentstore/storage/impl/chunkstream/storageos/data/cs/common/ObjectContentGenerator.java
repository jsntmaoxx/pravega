package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

public class ObjectContentGenerator {
    public static void fillContent(byte[] buffer) {
        for (var i = 0; i < buffer.length; ++i) {
            buffer[i] = contentByte(i);
        }
    }

    public static byte contentByte(int pos) {
        return (byte) ('a' + (pos >> 4) % 26);
    }

    public static byte contentByte(long pos) {
        return (byte) ('a' + (pos >> 4) % 26);
    }
}
