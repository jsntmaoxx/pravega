package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

import java.util.zip.CRC32;

public class CRC {
    public static native long crc32(long init_crc, long buf, long len);

    public static native long crc32PaddingZero(long init_crc, int len);

    public static long crc32OfPadding(byte value, int length) {
        CRC32 crc32 = new CRC32();
        for (var i = 0; i < length; ++i) {
            crc32.update(value);
        }
        return crc32.getValue();
    }
}
