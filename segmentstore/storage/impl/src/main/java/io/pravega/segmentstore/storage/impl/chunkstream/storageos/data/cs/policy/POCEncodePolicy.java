package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class POCEncodePolicy extends AbstractEncodePolicy {
    @Override
    protected String[] parseKey(String key) {
        return key.split("/");
    }

    @Override
    protected ImmutablePair<String[], Integer> parsePrefix(String prefix) {
        return new ImmutablePair<>(prefix.split("/"), null);
    }

}
