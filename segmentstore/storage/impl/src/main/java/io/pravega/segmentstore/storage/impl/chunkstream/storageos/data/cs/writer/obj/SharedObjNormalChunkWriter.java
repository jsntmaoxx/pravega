package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkPrefetch;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteNormalChunkHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class SharedObjNormalChunkWriter extends SharedObjWriter {
    public SharedObjNormalChunkWriter(ChunkPrefetch chunkPrefetch, Executor executor, CSConfiguration csConfig) {
        super(chunkPrefetch, executor, csConfig);
    }

    protected WriteChunkHandler createWriteChunkHandler(CSConfiguration csConfig, Executor executor) throws CSException {
        ChunkObject chunkObj;
        try {
            chunkObj = chunkPrefetch.fetchNormalChunk().get();
            log().debug("get chunk obj:{}", chunkObj.chunkIdStr());
        } catch (InterruptedException | ExecutionException e) {
            throw new CSException("fetch new normal chunk failed", e);
        }
        if (chunkObj == null) {
            log().error("fetch now normal chunk failed");
            return null;
        }
        log().debug("create write normal chunk handler, with chunk obj:{}", chunkObj.chunkIdStr());
        return new WriteNormalChunkHandler(chunkObj, this, executor, csConfig);
    }
}
