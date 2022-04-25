package io.pravega.segmentstore.storage.impl.chunkstream;

import io.pravega.segmentstore.storage.LogAddress;

public class StreamAddress extends LogAddress implements Comparable<StreamAddress> {

    //region Constructor

    /**
     * Creates a new instance of the StreamAddress class.
     *
     * @param offset The offset in the stream.
     */
    StreamAddress(long offset) {
        super(offset);
    }

    //endregion

    //region Comparable Implementation

    @Override
    public int hashCode() {
        return Long.hashCode(getSequence());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamAddress) {
            return this.compareTo((StreamAddress) obj) == 0;
        }

        return false;
    }

    @Override
    public int compareTo(StreamAddress address) {
        return Long.compare(getSequence(), address.getSequence());
    }

    //endregion
}
