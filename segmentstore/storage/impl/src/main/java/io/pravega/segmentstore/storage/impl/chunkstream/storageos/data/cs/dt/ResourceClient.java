package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;

import java.util.Collection;

public interface ResourceClient {
    Bucket fromName(String name);

    Collection<Bucket> listBuckets();
}
