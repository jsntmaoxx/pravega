package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration.writeSegmentHeaderFooterLen;

/**
 * Define chunk-specific feature, including chunk size, code number, data number and chunk id naming rule.
 * referred in Bucket.
 */
public class ChunkConfig {

    public final CSConfiguration csConfig;

    protected int maxCodeMatrixCache = 120;
    protected int maxDataSegmentCache = 120;

    protected int chunkSize;
    protected int writeSharedChunkThreshold;
    protected ECSchema ecSchema;

    /**
     * @param chunkStrSize
     * @param ecSchemaDataNumber
     * @param ecSchemaCodeNumber
     * @param csConfig
     */
    public ChunkConfig(String chunkStrSize, int ecSchemaDataNumber, int ecSchemaCodeNumber, CSConfiguration csConfig) {
        this.csConfig = csConfig;

        chunkSize = StringUtils.strSize2Int(chunkStrSize);
        this.writeSharedChunkThreshold = this.chunkSize - (this.chunkSize / csConfig.defaultIndexGranularity + 1) * writeSegmentHeaderFooterLen;

        if (ecSchemaDataNumber != -1 && ecSchemaCodeNumber != -1) {
            if (chunkSize % ecSchemaDataNumber != 0) {
                throw new CSRuntimeException("chunk size " + chunkSize + " can not be divisible by ecSchemaDataNumber " + ecSchemaDataNumber);
            }

            var segmentLength = chunkSize / ecSchemaDataNumber;
            if (segmentLength < csConfig.writeSegmentSize) {
                throw new CSRuntimeException("chunk size " + chunkSize + " is less than write segment size " + csConfig.writeSegmentSize);
            }

            //TODO: should all ECSchema instances share the maxCodeMatrixMemMB?
            var maxCodeMatrixCache = 120;
            maxCodeMatrixCache = Math.max(maxCodeMatrixCache, (int) (csConfig.maxCodeMatrixMemMB * 1024 * 1024L / (segmentLength * ecSchemaCodeNumber)));
            var maxDataSegmentCache = maxCodeMatrixCache;
            this.ecSchema = new ECSchema(ecSchemaDataNumber, ecSchemaCodeNumber, segmentLength, csConfig, maxCodeMatrixCache, maxDataSegmentCache);
        }
    }

    public void shutdown() {
        if (ecSchema != null) {
            ecSchema.shutdown();
        }
    }


    public int maxCodeMatrixCache() {
        return maxCodeMatrixCache;
    }


    public int maxDataSegmentCache() {
        return maxDataSegmentCache;
    }

    public ECSchema ecSchema() {
        return ecSchema;
    }

    public int writeSharedChunkThreshold() {
        return writeSharedChunkThreshold;
    }

    public void updateDefaultIndexGranularity() {
        this.writeSharedChunkThreshold = this.chunkSize - (this.chunkSize / CSConfiguration.defaultIndexGranularity + 1) * writeSegmentHeaderFooterLen;
    }

    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public String toString() {
        return "ChunkConfig{" +
               "maxCodeMatrixCache=" + maxCodeMatrixCache +
               ", maxDataSegmentCache=" + maxDataSegmentCache +
               ", chunkSize=" + chunkSize +
               ", writeSharedChunkThreshold=" + writeSharedChunkThreshold +
               ", ecSchema=" + ecSchema +
               '}';
    }
}
