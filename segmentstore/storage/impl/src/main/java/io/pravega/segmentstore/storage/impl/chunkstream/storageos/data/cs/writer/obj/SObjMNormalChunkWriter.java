package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkPrefetch;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.LocationCoder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteNormalChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class SObjMNormalChunkWriter extends SObjMChunkWriter {
    private static final Logger log = LoggerFactory.getLogger(SObjMNormalChunkWriter.class);

    public SObjMNormalChunkWriter(String objKey, InputDataChunkFetcher inputDataFetcher, ChunkObject chunkObj, ChunkPrefetch chunkPrefetch, Executor executor, LocationCoder.CodeType locationType, CSConfiguration csConfig, long startNs, long timeoutSeconds) {
        super(objKey, inputDataFetcher, chunkObj, chunkPrefetch, executor, locationType, csConfig, startNs, timeoutSeconds);
    }

    @Override
    protected WriteChunkHandler createWriteChunkHandler(ChunkObject chunkObj, ChunkConfig chunkConfig, Executor executor) {
        return new WriteNormalChunkHandler(chunkObj, this.inputDataFetcher, executor, chunkConfig.csConfig);
    }

    @Override
    protected void rotateChunk() throws CSException {
        try {
            var nc = chunkPrefetch.fetchNormalChunk().get();
            activeWriteChunkHandler.end();
            activeWriteChunkHandler = createWriteChunkHandler(nc, csConfig.defaultChunkConfig(), executor);
            writeChunkHandlers.add(activeWriteChunkHandler);
        } catch (Exception e) {
            log.error("write obj {} failed to rotate chunk, current chunk {}", objKey, activeWriteChunkHandler.chunkIdStr(), e);
            throw new CSException("failed to rotate chunk");
        }
    }
}
