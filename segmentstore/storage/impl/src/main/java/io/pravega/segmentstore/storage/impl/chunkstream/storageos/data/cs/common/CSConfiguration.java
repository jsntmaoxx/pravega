package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;

public class CSConfiguration {
    public static final int maxVarInt32Bytes = 5;
    public static final int metricsReportSeconds = 5;
    public static final int ObjectBucketIndex = 65000;
    public static final boolean testSyncReadStream = false;
    public static final boolean skipRepoCRC = false;
    public static final int maxCodeMatrixMemMB = 4096;
    public static final int maxChunkIdNumInStreamData = 128;
    public static final int streamPrefetchChunkCount = 2;
    public static final int streamDataCacheCapacity = 4096;
    private static final int nativeBufferNuma = -1; // -1 to disable
    static protected int writeSegmentSize = 2 * 1024 * 1024;
    static protected int readSegmentSize = 2 * 1024 * 1024;
    static protected int writeSegmentHeaderMagicLen = 0;
    static protected int writeSegmentHeaderSizeLen = 4;
    static protected int writeSegmentHeaderLen = writeSegmentHeaderMagicLen + writeSegmentHeaderSizeLen;
    static protected int writeSegmentFooterChecksumLen = 8;
    static protected int writeSegmentFooterEpochLen = 36;
    static protected int writeSegmentFooterLen = writeSegmentFooterChecksumLen + writeSegmentFooterEpochLen;
    static protected int writeSegmentHeaderFooterLen = writeSegmentHeaderLen + writeSegmentFooterLen;
    static protected int defaultIndexGranularity = 64 * 1024 - writeSegmentHeaderFooterLen;
    protected int initCodeMatrixCache = 5;
    protected int initDataSegmentCache = 5;
    protected ChunkConfigShelf chunkConfigShelf;
    protected ChunkConfig defaultChunkConfig;// used by ChunkPrefetch in PutObject flow
    protected int fetchChunkTimeoutSeconds = 120;
    protected int listChunkTimeoutSeconds = 120;
    protected int writeSharedObjectTimeoutSeconds = 120;
    protected int writeSharedObjectSegmentTimeoutSeconds = 3;
    protected int sharedObjWriterNumber = 1 << 3; // must power of 2
    protected int chunkIOThreadNumber = Math.max(8, StringUtils.roundToFloorPowerOf2(Runtime.getRuntime().availableProcessors() * 0.5));
    protected int ecThreadNumber = Math.max(8, StringUtils.roundToFloorPowerOf2(Runtime.getRuntime().availableProcessors() * 0.5));
    protected int eTagThreadNumber = Math.max(4, StringUtils.roundToFloorPowerOf2(Runtime.getRuntime().availableProcessors() * 0.2));
    protected int writeDiskTimeoutSeconds = 20;
    protected int readDiskTimeoutSeconds = 20;
    protected LocationCoder.CodeType locationCodeType = LocationCoder.CodeType.PrintJson;
    protected int createChunkTimeoutSeconds = 120;
    protected int freeBlockDelaySeconds = 15;
    protected int listMaxSize = 1000;
    protected int maxPrepareWriteBatchCountInChunk = 4;
    // debug option
    private boolean useNativeBuffer = true;
    private boolean skipEC = false;
    private boolean skipMD5 = false;
    private boolean skipWriteEC = false;
    private boolean skipShareMemDataCopy = false;
    private boolean skipNVMeRepoWrite = false;
    private boolean skipWriteFlow = false;

    public CSConfiguration(String defaultChunkStrSize, int defaultEcSchemaDataNumber, int defaultEcSchemaCodeNumber) {
        chunkConfigShelf = new ChunkConfigShelf(this);
        defaultChunkConfig = chunkConfigShelf.getChunkConfig(defaultChunkStrSize, defaultEcSchemaDataNumber, defaultEcSchemaCodeNumber);
    }

    public static int writeSegmentSize() {
        return writeSegmentSize;
    }

    public static int readSegmentSize() {
        return readSegmentSize;
    }

    public static int defaultIndexGranularity() {
        return defaultIndexGranularity;
    }

    public static int writeSegmentHeaderLen() {
        return writeSegmentHeaderLen;
    }

    public static int writeSegmentFooterLen() {
        return writeSegmentFooterLen;
    }

    public static int writeSegmentFooterChecksumLen() {
        return writeSegmentFooterChecksumLen;
    }

    public static int writeSegmentFooterEpochLen() {
        return writeSegmentFooterEpochLen;
    }

    public static int writeSegmentOverhead() {
        return writeSegmentHeaderFooterLen;
    }

    public static int nativeBufferNuma() {
        return nativeBufferNuma;
    }

    public static int chunkIdByteCapacityInStreamData() {
        return maxChunkIdNumInStreamData * Long.BYTES * 2;
    }

    public void shutdown() {
        chunkConfigShelf.shutdown();
    }

    @Override
    public String toString() {
        return new StringBuilder(1024).append("\n")
                                      .append("defaultChunkConfig                    ").append(defaultChunkConfig).append("\n")
                                      .append("defaultIndexGranularity      ").append(StringUtils.size2String(defaultIndexGranularity)).append("\n")
                                      .append("writeSegmentHeaderFooterLen  ").append(StringUtils.size2String(writeSegmentHeaderFooterLen)).append("\n")
//                                      .append("ecSchema                     ").append(ecSchema.DataNumber).append(" + ").append(ecSchema.CodeNumber).append("\n")
//                                      .append("maxCodeMatrixCache           ").append(maxCodeMatrixCache).append("\n")
//                                      .append("maxDataSegmentCache          ").append(maxDataSegmentCache).append("\n")
                                      .append("chunkIOThreadNumber          ").append(chunkIOThreadNumber).append("\n")
                                      .append("ecThreadNumber               ").append(ecThreadNumber).append("\n")
                                      .append("eTagThreadNumber             ").append(eTagThreadNumber).append("\n")
                                      // debug options
                                      .append("debug-useNativeBuffer        ").append(useNativeBuffer).append("\n")
                                      .append("debug-nativeBufferNuma       ").append(nativeBufferNuma).append("\n")
                                      .append("debug-skipEC                 ").append(skipEC).append("\n")
                                      .append("debug-skipMD5                ").append(skipMD5).append("\n")
                                      .append("debug-skipWriteEC            ").append(skipWriteEC).append("\n")
                                      .append("debug-skipShareMemDataCopy   ").append(skipShareMemDataCopy).append("\n")
                                      .append("debug-skipNVMeRepoWrite      ").append(skipNVMeRepoWrite).append("\n")
                                      .append("debug-skipWriteFlow          ").append(skipWriteFlow).append("\n")
                                      .append("debug-syncReadStream         ").append(testSyncReadStream).append("\n")
                                      .append("debug-skipRepoCRC            ").append(skipRepoCRC).append("\n")
                                      .toString();
    }


    public int initCodeMatrixCache() {
        return initCodeMatrixCache;
    }

    public int initDataSegmentCache() {
        return initDataSegmentCache;
    }

    public int fetchChunkTimeoutSeconds() {
        return fetchChunkTimeoutSeconds;
    }

    public int listChunkTimeoutSeconds() {
        return listChunkTimeoutSeconds;
    }

    public int writeSharedObjectTimeoutSeconds() {
        return writeSharedObjectTimeoutSeconds;
    }

    public int writeSharedObjectSegmentTimeoutSeconds() {
        return writeSharedObjectSegmentTimeoutSeconds;
    }

    public int sharedObjWriterNumber() {
        return sharedObjWriterNumber;
    }

    public int chunkIOThreadNumber() {
        return chunkIOThreadNumber;
    }

    public int ecThreadNumber() {
        return ecThreadNumber;
    }

    public int eTagThreadNumber() {
        return eTagThreadNumber;
    }

    public int writeDiskTimeoutSeconds() {
        return writeDiskTimeoutSeconds;
    }

    public int readDiskTimeoutSeconds() {
        return readDiskTimeoutSeconds;
    }

    public LocationCoder.CodeType locationCodeType() {
        return locationCodeType;
    }

    public int createChunkTimeoutSeconds() {
        return createChunkTimeoutSeconds;
    }

    public int freeBlockDelaySeconds() {
        return freeBlockDelaySeconds;
    }

    public int listMaxSize() {
        return listMaxSize;
    }

    public int maxPrepareWriteBatchCountInChunk() {
        return maxPrepareWriteBatchCountInChunk;
    }

    public boolean useNativeBuffer() {
        return useNativeBuffer;
    }

    public void useNativeBuffer(boolean useNativeBuffer) {
        this.useNativeBuffer = useNativeBuffer;
    }

    public boolean skipEC() {
        return skipEC;
    }

    public void skipEC(boolean skipEC) {
        this.skipEC = skipEC;
    }

    public boolean skipMD5() {
        return skipMD5;
    }

    public void skipMD5(boolean skipMD5) {
        this.skipMD5 = skipMD5;
    }

    public boolean skipWriteEC() {
        return skipWriteEC;
    }

    public void skipWriteEC(boolean skipWriteECCode) {
        this.skipWriteEC = skipWriteECCode;
    }

    public void skipShareMemDataCopy(boolean skipShareMemDataCopy) {
        this.skipShareMemDataCopy = skipShareMemDataCopy;
    }

    public boolean skipNVMeRepoWrite() {
        return skipNVMeRepoWrite;
    }

    public void skipNVMeRepoWrite(boolean skipRepoWrite) {
        this.skipNVMeRepoWrite = skipRepoWrite;
    }

    public boolean skipWriteFlow() {
        return skipWriteFlow;
    }

    public void skipWriteFlow(boolean skipWriteFlow) {
        this.skipWriteFlow = skipWriteFlow;
    }

    public void updateRpcConfig(RpcConfiguration rpcConfig) {
        rpcConfig.skipShareMemDataCopy(this.skipShareMemDataCopy);
    }

    public ChunkConfigShelf chunkConfigShelf() {
        return chunkConfigShelf;
    }

    public ChunkConfig defaultChunkConfig() {
        return defaultChunkConfig;
    }
}
