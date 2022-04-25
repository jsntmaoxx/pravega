package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy.AbstractEncodePolicy;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy.HEXEncodePolicy;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy.ObjectEncodePolicy;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy.POCEncodePolicy;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.bucket.Bucket.NamingRule;

public class Bucket {
    public final String name;
    public final int index;
    public final boolean isChunkObjectEnabled;
    public final NamingRule namingRule;
    private final AbstractEncodePolicy encodeStrategystrategy;
//    public String rg;

    public ChunkConfig chunkConfig;

    public Bucket(String name, int index, CSConfiguration csConfig) {
        this.isChunkObjectEnabled = false;
        this.name = name;
        this.index = index;
        this.namingRule = NamingRule.POC_KEY_RULE;
        this.chunkConfig = csConfig.defaultChunkConfig();
        encodeStrategystrategy = new POCEncodePolicy();
    }

    public Bucket(String name, int index, String chunkSize, int ecDataNumber, int ecCodeNumber, NamingRule namingRule, CSConfiguration csConfig) {
        this.isChunkObjectEnabled = true;
        this.name = name;
        this.index = index;
        this.namingRule = namingRule;
        chunkConfig = csConfig.chunkConfigShelf().getChunkConfig(chunkSize, ecDataNumber, ecCodeNumber);
        switch (namingRule) {
            case OBJECT_KEY_RULE:
                encodeStrategystrategy = new ObjectEncodePolicy();
                break;
            case HEX_KEY_RULE:
                encodeStrategystrategy = new HEXEncodePolicy();
                break;
            case POC_KEY_RULE:
                encodeStrategystrategy = new POCEncodePolicy();
                break;
            default:
                encodeStrategystrategy = new POCEncodePolicy();
        }
    }

    public Bucket(String name, int index, String chunkSize, NamingRule namingRule, CSConfiguration csConfig) {
        this(name, index, chunkSize, -1, -1, namingRule, csConfig);
    }

    public boolean isChunkObjectEnabled() {
        return isChunkObjectEnabled;
    }

    public AbstractEncodePolicy getEncodeStrategystrategy() {
        return encodeStrategystrategy;
    }


    @Override
    public String toString() {
        return "Bucket{" +
               "name='" + name +
               ", index=" + index +
               ", chunkConfig=" + chunkConfig +
               ", namingRule=" + namingRule +
               '}';
    }
}
