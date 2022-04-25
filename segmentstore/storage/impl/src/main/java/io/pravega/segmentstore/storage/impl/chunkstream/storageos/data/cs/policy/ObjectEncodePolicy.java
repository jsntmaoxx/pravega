package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class ObjectEncodePolicy extends AbstractEncodePolicy {
    @Override
    protected String[] parseKey(String key) {
        return new String[0];
    }

    @Override
    protected ImmutablePair<String[], Integer> parsePrefix(String prefix) {
        return null;
    }
}
