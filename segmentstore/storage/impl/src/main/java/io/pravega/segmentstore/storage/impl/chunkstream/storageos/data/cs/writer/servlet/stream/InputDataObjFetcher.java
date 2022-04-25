package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

public interface InputDataObjFetcher extends Comparable<InputDataObjFetcher> {
    void onFetchObjData();

    String name();
}
