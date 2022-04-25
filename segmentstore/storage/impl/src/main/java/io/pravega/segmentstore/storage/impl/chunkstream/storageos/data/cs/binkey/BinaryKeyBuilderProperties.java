package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.binkey;

public class BinaryKeyBuilderProperties {

    //    @Value("#{objectProperties['object.exceptionOnPrefixCorruption']}")
    private static boolean exceptionOnPrefixCorruption = true;

    public static boolean isExceptionOnPrefixCorruption() {
        return exceptionOnPrefixCorruption;
    }

    public static void setExceptionOnPrefixCorruption(boolean exceptionOnPrefixCorruption) {
        BinaryKeyBuilderProperties.exceptionOnPrefixCorruption = exceptionOnPrefixCorruption;
    }
}
