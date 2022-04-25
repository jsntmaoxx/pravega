package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

public class SizeUtils {
    public static int estimateChunkLength(int dataSize, int indexGranularity) {
        assert indexGranularity > 0;
        var writeSize = indexGranularity + CSConfiguration.writeSegmentOverhead();
        var overhead = dataSize / writeSize * CSConfiguration.writeSegmentOverhead();
        if (dataSize % writeSize > 0) {
            overhead += CSConfiguration.writeSegmentOverhead();
        }
        return dataSize + overhead;
    }

    public static int estimateDataLength(int chunkSize, int indexGranularity) {
        assert indexGranularity > 0;
        var writeSize = indexGranularity + CSConfiguration.writeSegmentOverhead();
        var overhead = chunkSize / writeSize * CSConfiguration.writeSegmentOverhead();
        if (chunkSize % writeSize > 0) {
            overhead += CSConfiguration.writeSegmentOverhead();
        }
        return chunkSize - overhead;
    }

    public static int aroundToPowerOf2(int num) {
        var v = 1;
        while (v < num) {
            v <<= 1;
        }
        return v;
    }

    public static long modMask(long num) {
        var m = 0L;
        while (num > 1) {
            m = (m << 1) + 1;
            num >>>= 1;
        }
        return m;
    }
}
