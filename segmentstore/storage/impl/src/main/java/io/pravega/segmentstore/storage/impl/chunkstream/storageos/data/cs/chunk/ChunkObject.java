package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.range.ChunkSegmentRange;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface ChunkObject extends Comparable {

    UUID chunkUUID();

    String chunkIdStr();

    int chunkSize();

    void triggerSeal(int length);

    int offset();

    boolean triggerSeal();

    int sealLength();

    CompletableFuture<Location> write(WriteBatchBuffers batchBuffers, Executor executor) throws Exception;

    void read(ChunkSegmentRange segRange, Executor executor, CompletableFuture<ReadDataBuffer> readFuture) throws Exception;

    CmMessage.ChunkInfo chunkInfo();

    void chunkInfo(CmMessage.ChunkInfo chunkInfo);

    CompletableFuture<Void> writeNativeEC(CmMessage.CopyOrBuilder ecCopy, long codeBufferAddress, int codeNumber, int segmentLength, Executor executor);

    CompletableFuture<Void> writeNativeECSegment(CmMessage.SegmentInfo seg, int codeSegIndex, ByteBuf segByteBuf, int segmentLength, Executor executor) throws Exception;

    int writeGranularity();

    int contentGranularity();

    int chunkLength();

    CmMessage.Copy findECCopy();

    CompletableFuture<ChunkObject> completeClientEC(int sealLength, CmMessage.Copy ecCopy) throws CSException, IOException;

    CmMessage.SegmentInfo findCopySegments(CmMessage.CopyOrBuilder ecCopy, int ecSegIndex);

    int writingOffset();
}
