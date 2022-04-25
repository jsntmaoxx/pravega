package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range;

public enum RangeType {
    Object("object"),
    ByteRange("bytes");

    private final String name;

    RangeType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }

}
