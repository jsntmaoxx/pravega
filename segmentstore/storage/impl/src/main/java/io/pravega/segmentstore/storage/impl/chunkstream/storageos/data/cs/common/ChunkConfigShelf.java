package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;
import java.util.Map;

/**
 * Keep a rack/shelf of ChunkConfig.
 * referred in Bucket
 */
public class ChunkConfigShelf {

    private CSConfiguration csConfig;

    private Map<Triple<String, Integer, Integer>, ChunkConfig> chunkConfigMap = new HashMap<>();

    public ChunkConfigShelf(CSConfiguration csConfig) {
        this.csConfig = csConfig;
    }

    public ChunkConfig getChunkConfig(String chunkSizeStr, int dataNumber, int codeNumber) {
        var key = Triple.of(chunkSizeStr, dataNumber, codeNumber);
        var chunkConfig = chunkConfigMap.computeIfAbsent(key, triple -> new ChunkConfig(chunkSizeStr, dataNumber, codeNumber, csConfig));
        return chunkConfig;
    }

    public void updateDefaultIndexGranularity() {
        chunkConfigMap.values().forEach(chunkConfig -> chunkConfig.updateDefaultIndexGranularity());
    }

    public void shutdown() {
        chunkConfigMap.values().forEach(chunkConfig -> chunkConfig.shutdown());
    }

}
