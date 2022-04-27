package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.TestSettings;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.ssm.SsmMessage;

import java.io.IOException;
import java.util.List;

public abstract class ChunkGenerator {
    protected final CSConfiguration csConfig;

    protected ChunkGenerator(CSConfiguration csConfig) {
        this.csConfig = csConfig;
    }

    private static CmMessage.SSLocation constructSSLocation(SsmMessage.BlockAllocateResponse response) {
        var seg = response.getSegments(0);
        return CmMessage.SSLocation.newBuilder()
                                   .setSsId(response.getSsId())
                                   .setPartitionId(response.getPartitionId())
                                   .setFilename(seg.getBlockBinId())
                                   .setOffset(seg.getOffset())
                                   .setStatus(CmMessage.BlockStatus.HEALTHY)
                                   .setTag(seg.getTag())
                                   .setStatusSn(0)
                                   .build();
    }

    public CmMessage.ChunkInfo createNormalChunk(String chunkId, int indexGranularity, ChunkConfig chunkConfig) throws IOException {
        return createNormalChunk(chunkId, indexGranularity, CmMessage.ChunkStatus.ACTIVE, chunkConfig);
    }

    public CmMessage.ChunkInfo createNormalChunk(String chunkId, int indexGranularity, CmMessage.ChunkStatus status, ChunkConfig chunkConfig) throws IOException {
        CmMessage.ChunkInfo.Builder chunkInfoBuilder = CmMessage.ChunkInfo.newBuilder()
                                                                          .setStatus(status)
                                                                          .setDataType(CmMessage.ChunkDataType.REPO)
                                                                          .setSequenceNumber(1L)
                                                                          .setMinNotSealedSequenceNumber(50L)
                                                                          .setType(CmMessage.ChunkType.LOCAL)
                                                                          .setCapacity(chunkConfig.chunkSize())
                                                                          .setLastKnownLength(0)
                                                                          .setIndexGranularity(indexGranularity);
        if (status != CmMessage.ChunkStatus.ACTIVE) {
            chunkInfoBuilder.setSealedLength(chunkConfig.chunkSize());
        }
        if (TestSettings.geoSendEnabled) {
            chunkInfoBuilder.addSecondaries(CmMessage.SecondaryPair.newBuilder()
                                                                   .setSecondary("VDC2")
                                                                   .setReplicated(true)
                                                                   .build())
                            .setRepGroup("RG");
        }
        addNormalCopies(chunkId, chunkInfoBuilder, 3, chunkConfig.chunkSize());
        return chunkInfoBuilder.build();
    }

    public CmMessage.ChunkInfo createType1Chunk(String chunkId, int indexGranularity, ChunkConfig chunkConfig) throws IOException {
        return createType1Chunk(chunkId, indexGranularity, CmMessage.ChunkStatus.ACTIVE, chunkConfig);
    }

    public CmMessage.ChunkInfo createNormalChunk(ECSchema ecSchema, String chunkId, int indexGranularity, ChunkConfig chunkConfig) throws IOException {
        return createNormalChunk(ecSchema, chunkId, indexGranularity, CmMessage.ChunkStatus.ACTIVE, chunkConfig);
    }

    public CmMessage.ChunkInfo createNormalChunk(ECSchema ecSchema, String chunkId, int indexGranularity, CmMessage.ChunkStatus status, ChunkConfig chunkConfig) throws IOException {
        CmMessage.ChunkInfo.Builder chunkInfoBuilder = CmMessage.ChunkInfo.newBuilder()
                                                                          .setStatus(status)
                                                                          .setDataType(CmMessage.ChunkDataType.REPO)
                                                                          .setSequenceNumber(1L)
                                                                          .setMinNotSealedSequenceNumber(50L)
                                                                          .setType(CmMessage.ChunkType.LOCAL)
                                                                          .setCapacity(chunkConfig.chunkSize())
                                                                          .setLastKnownLength(0)
                                                                          .setIndexGranularity(indexGranularity);
        if (status != CmMessage.ChunkStatus.ACTIVE) {
            chunkInfoBuilder.setSealedLength(chunkConfig.chunkSize());
        }
        if (TestSettings.geoSendEnabled) {
            chunkInfoBuilder.addSecondaries(CmMessage.SecondaryPair.newBuilder()
                                                                   .setSecondary("VDC2")
                                                                   .setReplicated(true)
                                                                   .build())
                            .setRepGroup("RG");
        }
        addNormalCopies(ecSchema, chunkId, chunkInfoBuilder, 3);
        return chunkInfoBuilder.build();
    }

    public CmMessage.ChunkInfo createType1Chunk(String chunkId, int indexGranularity, CmMessage.ChunkStatus status, ChunkConfig chunkConfig) throws IOException {
        CmMessage.ChunkInfo.Builder chunkInfoBuilder = CmMessage.ChunkInfo.newBuilder()
                                                                          .setStatus(status)
                                                                          .setDataType(CmMessage.ChunkDataType.REPO)
                                                                          .setSequenceNumber(1L)
                                                                          .setMinNotSealedSequenceNumber(50L)
                                                                          .setType(CmMessage.ChunkType.LOCAL)
                                                                          .setCapacity(chunkConfig.chunkSize())
                                                                          .setLastKnownLength(0)
                                                                          .setIndexGranularity(indexGranularity);
        if (status != CmMessage.ChunkStatus.ACTIVE) {
            chunkInfoBuilder.setSealedLength(chunkConfig.chunkSize());
        }
        if (TestSettings.geoSendEnabled) {
            chunkInfoBuilder.addSecondaries(CmMessage.SecondaryPair.newBuilder()
                                                                   .setSecondary("VDC2")
                                                                   .setReplicated(true)
                                                                   .build())
                            .setRepGroup("RG");
        }
        addNormalCopies(chunkId, chunkInfoBuilder, 2, chunkConfig.chunkSize());
        addECCopy(chunkConfig.ecSchema(), chunkId, chunkInfoBuilder);
        return chunkInfoBuilder.build();
    }

    private void addNormalCopies(String chunkId, CmMessage.ChunkInfo.Builder chunkInfoBuilder, int count, int chunkSize) throws IOException {
        for (SsmMessage.BlockAllocateResponse response : allocateNormalBlocks(chunkId, count)) {
            chunkInfoBuilder.addCopies(CmMessage.Copy.newBuilder()
                                                     .setIsEc(false)
                                                     .addSegments(CmMessage.SegmentInfo.newBuilder()
                                                                                       .setLocationType(CmMessage.LocationType.STORAGE_SERVER)
                                                                                       .setSsLocation(constructSSLocation(response))
                                                                                       .setOffset(0)
                                                                                       .setEndOffset(chunkSize)));
        }
    }

    private void addNormalCopies(ECSchema ecSchema, String chunkId, CmMessage.ChunkInfo.Builder chunkInfoBuilder, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            var copy = CmMessage.Copy.newBuilder().setIsEc(false);
            var seq = 0;
            for (SsmMessage.BlockAllocateResponse response : allocateNormalBlocks(ecSchema, chunkId, i)) {
                copy.addSegments(CmMessage.SegmentInfo.newBuilder()
                                                      .setLocationType(CmMessage.LocationType.STORAGE_SERVER)
                                                      .setSsLocation(constructSSLocation(response))
                                                      .setOffset(0)
                                                      .setEndOffset(ecSchema.SegmentLength)
                                                      .setSequence(seq++));
            }
            chunkInfoBuilder.addCopies(copy);
        }
    }

    private void addECCopy(ECSchema ecSchema, String chunkId, CmMessage.ChunkInfo.Builder chunkInfoBuilder) throws IOException {
        var ecCopy = CmMessage.Copy.newBuilder().setIsEc(true).setIsClientCreatedEcCopy(true);
        var seq = 0;
        for (SsmMessage.BlockAllocateResponse response : allocateECBlocks(ecSchema, chunkId)) {
            ecCopy.addSegments(CmMessage.SegmentInfo.newBuilder()
                                                    .setLocationType(CmMessage.LocationType.STORAGE_SERVER)
                                                    .setSsLocation(constructSSLocation(response))
                                                    .setOffset(0)
                                                    .setEndOffset(ecSchema.SegmentLength)
                                                    .setSequence(seq++));
        }
        chunkInfoBuilder.addCopies(ecCopy);
    }

    public CmMessage.ChunkInfo createType2Chunk(String chunkId, int indexGranularity, ChunkConfig chunkConfig) throws IOException {
        return createType2Chunk(chunkId, indexGranularity, CmMessage.ChunkStatus.ACTIVE, chunkConfig);
    }

    public CmMessage.ChunkInfo createType2Chunk(String chunkId, int indexGranularity, CmMessage.ChunkStatus status, ChunkConfig chunkConfig) throws IOException {
        return createType2Chunk(chunkId, indexGranularity, status, chunkConfig, null);
    }

    public CmMessage.ChunkInfo createType2Chunk(String chunkId, int indexGranularity, CmMessage.ChunkStatus status, ChunkConfig chunkConfig, String objName) throws IOException {
        CmMessage.ChunkInfo.Builder chunkInfoBuilder = CmMessage.ChunkInfo.newBuilder()
                                                                          .setStatus(status)
                                                                          .setDataType(CmMessage.ChunkDataType.REPO)
                                                                          .setSequenceNumber(1L)
                                                                          .setMinNotSealedSequenceNumber(50L)
                                                                          .setType(CmMessage.ChunkType.LOCAL)
                                                                          .setCapacity(chunkConfig.chunkSize())
                                                                          .setLastKnownLength(0)
                                                                          .setIndexGranularity(indexGranularity);
        if (status != CmMessage.ChunkStatus.ACTIVE) {
            chunkInfoBuilder.setSealedLength(chunkConfig.chunkSize());
        }
        if (objName != null) {
            chunkInfoBuilder.setOriginalKey(objName);
        }
        if (TestSettings.geoSendEnabled) {
            chunkInfoBuilder.addSecondaries(CmMessage.SecondaryPair.newBuilder()
                                                                   .setSecondary("VDC2")
                                                                   .setReplicated(true)
                                                                   .build())
                            .setRepGroup("RG");
        }
        addECCopy(chunkConfig.ecSchema(), chunkId, chunkInfoBuilder);
        return chunkInfoBuilder.build();
    }

    protected abstract List<SsmMessage.BlockAllocateResponse> allocateNormalBlocks(String chunkId, int copyCount) throws IOException;

    protected abstract List<SsmMessage.BlockAllocateResponse> allocateNormalBlocks(ECSchema ecSchema, String chunkId, int copyIndex) throws IOException;

    protected abstract List<SsmMessage.BlockAllocateResponse> allocateECBlocks(ECSchema ecSchema, String chunkId) throws IOException;
}
