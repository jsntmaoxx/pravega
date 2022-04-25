package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.bucket.Bucket.Entry;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.bucket.Bucket.StringMap;

import java.util.HashMap;
import java.util.Map;

public class BucketGPBFactory {

    public static Map<String, String> toStringMap(StringMap ipcMap) {
        Map<String, String> kpMap = null;

        if (ipcMap != null) {
            kpMap = new HashMap<>();
            if (ipcMap.getRemovedCount() > 0) {
                for (Entry e : ipcMap.getRemovedList()) {
                    kpMap.put(e.getKey(), e.getValue());
                    kpMap.remove(e.getKey());
                }
            }
            if (ipcMap.getAddedCount() > 0) {
                for (Entry e : ipcMap.getAddedList()) {
                    kpMap.put(e.getKey(), e.getValue());
                }

            }
        }
        return kpMap;
    }
}
