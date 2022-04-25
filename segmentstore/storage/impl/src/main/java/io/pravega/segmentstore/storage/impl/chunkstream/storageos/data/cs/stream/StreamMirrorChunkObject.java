package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.NormalChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;

import java.util.UUID;

public class StreamMirrorChunkObject extends NormalChunkObject implements StreamChunkObject {
    private final long streamSequence;
    private final int chunkOrder;
    private long streamStartLogicalOffset = -1;
    private long streamEndLogicalOffset = -1;
    private long streamStartPhysicalOffset = -1;
    private long streamEndPhysicalOffset = -1;

    public StreamMirrorChunkObject(long streamSequence,
                                   UUID chunkId,
                                   CmMessage.ChunkInfo chunkInfo,
                                   int chunkOrder,
                                   DiskClient<DiskMessage> rpcClient,
                                   CmClient cmClient,
                                   ChunkConfig chunkConfig) {
        super(chunkId, chunkInfo, rpcClient, cmClient, chunkConfig);
        this.streamSequence = streamSequence;
        this.chunkOrder = chunkOrder;
    }

    @Override
    public long streamSequence() {
        return streamSequence;
    }

    @Override
    public int chunkOrder() {
        return chunkOrder;
    }

    @Override
    public void updateStreamStartOffset(long logicalOffset, long physicalOffset) {
        this.streamStartLogicalOffset = logicalOffset;
        this.streamStartPhysicalOffset = physicalOffset;
    }

    @Override
    public void updateStreamEndOffset(long logicalOffset, long physicalOffset) {
        this.streamEndLogicalOffset = logicalOffset;
        this.streamEndPhysicalOffset = physicalOffset;
    }

    @Override
    public long streamStartLogicalOffset() {
        return streamStartLogicalOffset;
    }

    @Override
    public long streamEndLogicalOffset() {
        return streamEndLogicalOffset;
    }

    @Override
    public long streamStartPhysicalOffset() {
        return streamStartPhysicalOffset;
    }

    @Override
    public long streamEndPhysicalOffset() {
        return streamEndPhysicalOffset;
    }
}
