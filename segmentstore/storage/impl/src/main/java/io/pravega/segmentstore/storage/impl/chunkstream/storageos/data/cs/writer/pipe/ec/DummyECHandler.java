package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class DummyECHandler extends ECHandler {
    private final CompletableFuture<CmMessage.Copy> ecFuture;
    private final AtomicBoolean ended = new AtomicBoolean(false);

    public DummyECHandler(ChunkObject chunkObject, Long codeMatrix, Long segBuffer, ChunkConfig chunkConfig) {
        super(codeMatrix, segBuffer, chunkConfig, chunkObject);
        this.ecFuture = new CompletableFuture<>();
    }

    @Override
    public boolean offer(WriteBatchBuffers batchBuffer) {
        return true;
    }

    @Override
    public boolean offer(WriteDataBuffer buffer) throws CSException {
        return true;
    }

    @Override
    public void end() {
        if (!ended.compareAndSet(false, true)) {
            return;
        }
        this.ecFuture.complete(chunkObject.findECCopy());
        releaseResource();
    }

    @Override
    public void errorEnd() {
        if (ecFuture.isDone()) {
            return;
        }
        ecFuture.completeExceptionally(new CSException("Dummy EC was ended with error"));
        releaseResource();
    }

    @Override
    public void cancel() {
    }

    @Override
    public CompletableFuture<CmMessage.Copy> ecFuture() {
        return ecFuture;
    }
}
