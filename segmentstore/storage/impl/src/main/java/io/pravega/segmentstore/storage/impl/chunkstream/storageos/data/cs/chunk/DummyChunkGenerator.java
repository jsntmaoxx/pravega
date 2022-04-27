package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.TestSettings;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.ssm.SsmMessage.BlockAllocateResponse;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.ssm.SsmMessage.BlockOperationStatus;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.ssm.SsmMessage.BlockSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DummyChunkGenerator extends ChunkGenerator {
    private static final String partitionPrefix = "PD-";
    private static final String[] deviceIds = new String[]{
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()};
    private static volatile int currentBlockBin = 0;
    private static volatile long currentBlockBinOffset = 0;

    public DummyChunkGenerator(CSConfiguration csConfig) {
        super(csConfig);
    }

    @Override
    protected synchronized List<BlockAllocateResponse> allocateNormalBlocks(String chunkId, int copyCount) {
        if (currentBlockBinOffset + csConfig.defaultChunkConfig().chunkSize() > TestSettings.BlockBinSize) {
            currentBlockBinOffset = 0;
            ++currentBlockBin;
        }

        List<BlockAllocateResponse> allocateResponseList = new ArrayList<>();
        for (int i = 0; i < copyCount; ++i) {
            allocateResponseList.add(BlockAllocateResponse.newBuilder()
                                                          .setStatus(BlockOperationStatus.SUCCESS)
                                                          .setSsId(deviceIds[i])
                                                          .setPartitionId(partitionPrefix + i)
                                                          .addSegments(BlockSegment.newBuilder()
                                                                                   .setSize(csConfig.defaultChunkConfig().chunkSize())
                                                                                   .setBlockBinId(String.format("%04d", currentBlockBin))
                                                                                   .setOffset(currentBlockBinOffset)
                                                                                   .setTag(System.currentTimeMillis())
                                                                                   .build())
                                                          .build());
        }
        currentBlockBinOffset += csConfig.defaultChunkConfig().chunkSize();
        return allocateResponseList;
    }

    @Override
    protected synchronized List<BlockAllocateResponse> allocateNormalBlocks(ECSchema ecSchema, String chunkId, int copyIndex) {
        if (currentBlockBinOffset + ecSchema.SegmentLength > TestSettings.BlockBinSize) {
            currentBlockBinOffset = 0;
            ++currentBlockBin;
        }

        List<BlockAllocateResponse> allocateResponseList = new ArrayList<>();
        for (int i = 0; i < ecSchema.DataNumber; ++i) {
            allocateResponseList.add(BlockAllocateResponse.newBuilder()
                                                          .setStatus(BlockOperationStatus.SUCCESS)
                                                          .setSsId(deviceIds[i % deviceIds.length])
                                                          .setPartitionId(partitionPrefix + copyIndex + i)
                                                          .addSegments(BlockSegment.newBuilder()
                                                                                   .setSize(ecSchema.SegmentLength)
                                                                                   .setBlockBinId(String.format("%04d", currentBlockBin))
                                                                                   .setOffset(currentBlockBinOffset)
                                                                                   .setTag(System.currentTimeMillis())
                                                                                   .build())
                                                          .build());
        }
        currentBlockBinOffset += ecSchema.SegmentLength;
        return allocateResponseList;
    }

    @Override
    protected synchronized List<BlockAllocateResponse> allocateECBlocks(ECSchema ecSchema, String chunkId) {
        if (currentBlockBinOffset + ecSchema.SegmentLength > TestSettings.BlockBinSize) {
            currentBlockBinOffset = 0;
            ++currentBlockBin;
        }

        List<BlockAllocateResponse> allocateResponseList = new ArrayList<>();
        for (int i = 0; i < ecSchema.DataNumber + ecSchema.CodeNumber; ++i) {
            allocateResponseList.add(BlockAllocateResponse.newBuilder()
                                                          .setStatus(BlockOperationStatus.SUCCESS)
                                                          .setSsId(deviceIds[i % deviceIds.length])
                                                          .setPartitionId(partitionPrefix + i)
                                                          .addSegments(BlockSegment.newBuilder()
                                                                                   .setSize(ecSchema.SegmentLength)
                                                                                   .setBlockBinId(String.format("%04d", currentBlockBin))
                                                                                   .setOffset(currentBlockBinOffset)
                                                                                   .setTag(System.currentTimeMillis())
                                                                                   .build())
                                                          .build());
        }
        currentBlockBinOffset += ecSchema.SegmentLength;
        return allocateResponseList;
    }

}