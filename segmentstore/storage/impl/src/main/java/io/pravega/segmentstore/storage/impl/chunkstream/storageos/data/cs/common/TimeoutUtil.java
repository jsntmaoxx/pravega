package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

public class TimeoutUtil {
    static public long asyncReadSocketStreamTimeoutSeconds(long contentLength, CSConfiguration csConfig) {
        return csConfig.writeSharedObjectTimeoutSeconds() +
               contentLength / CSConfiguration.defaultIndexGranularity() * csConfig.writeSharedObjectSegmentTimeoutSeconds();
    }

    static public long asyncWriteDiskTimeoutSeconds(long contentLength, CSConfiguration csConfig) {
        return csConfig.writeSharedObjectTimeoutSeconds() +
               contentLength / CSConfiguration.defaultIndexGranularity() * csConfig.writeSharedObjectSegmentTimeoutSeconds();
    }

    static public long asyncWriteSocketStreamTimeoutSeconds(long contentLength, CSConfiguration csConfig) {
        return csConfig.writeSharedObjectTimeoutSeconds() +
               contentLength / CSConfiguration.defaultIndexGranularity() * csConfig.writeSharedObjectSegmentTimeoutSeconds();
    }
}
