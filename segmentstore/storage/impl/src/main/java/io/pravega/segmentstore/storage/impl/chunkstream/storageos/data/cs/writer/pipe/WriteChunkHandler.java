package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Location;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.slf4j.Logger;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class WriteChunkHandler implements InputDataBufferHandler {
    protected final ChunkObject chunkObj;
    protected final Queue<WriteBatchBuffers> readyBatches = new ConcurrentLinkedQueue<>();
    protected final Executor executor;
    protected final AtomicBoolean inWrite = new AtomicBoolean(false);
    protected final AtomicReference<WriteBatchBuffers> activeBatch;
    protected volatile boolean disableReceiveBuffer = false;

    protected WriteChunkHandler(ChunkObject chunkObj, Executor executor) {
        this.chunkObj = chunkObj;
        this.executor = executor;
        this.activeBatch = new AtomicReference<>(newBatchBuffer());
    }

    @Override
    public void end() {
        disableReceiveBuffer = true;
        if (finishedAllWrite()) {
            triggerSealChunk("finish write all data at end", null);
        } else {
            writeNext();
        }
    }

    protected abstract boolean finishedAllWrite();

    public String chunkIdStr() {
        return chunkObj.chunkIdStr();
    }

    public ChunkObject chunkObj() {
        return chunkObj;
    }

    public abstract CompletableFuture<Void> writeChunkFuture();

    public abstract boolean writeNext();

    public abstract Location getWriteLocation();

    public abstract boolean requireData();

    public abstract boolean acceptData();

    protected abstract Logger log();


    protected void triggerSealChunk(String reason, Throwable t) {
        var trigger = chunkObj.triggerSeal();
        if (t == null) {
            log().info("trigger seal chunk {} at length {}, trigger {}, accept data: {}, disable write: {}, reason: {}",
                       chunkObj.chunkIdStr(), chunkObj.chunkLength(), trigger, acceptDataOffset(), disableReceiveBuffer, reason);
        } else {
            log().error("trigger seal chunk {} at length {}, trigger {}, accept data: {}, disable write: {}, reason: {}",
                        chunkObj.chunkIdStr(), chunkObj.chunkLength(), trigger, acceptDataOffset(), disableReceiveBuffer, reason, t);
        }
    }

    protected abstract int acceptDataOffset();

    protected abstract void acceptBuffer(WriteDataBuffer buffer);

    protected abstract WriteBatchBuffers poll();

    protected abstract WriteBatchBuffers newBatchBuffer();
}
