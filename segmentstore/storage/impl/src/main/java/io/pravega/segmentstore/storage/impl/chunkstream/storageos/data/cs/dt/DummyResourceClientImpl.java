package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class DummyResourceClientImpl implements ResourceClient {
    private HashMap<String, Bucket> nameToBuckets;

    public void setBuckets(List<Bucket> nameToBuckets) {
        this.nameToBuckets = new HashMap<>((int) (nameToBuckets.size() / 0.75 + 1));
        for (var b : nameToBuckets) {
            this.nameToBuckets.put(b.name, b);
        }
    }

    @Override
    public Bucket fromName(String name) {
        return nameToBuckets.get(name);
    }

    @Override
    public Collection<Bucket> listBuckets() {
        return nameToBuckets.values();
    }
}
