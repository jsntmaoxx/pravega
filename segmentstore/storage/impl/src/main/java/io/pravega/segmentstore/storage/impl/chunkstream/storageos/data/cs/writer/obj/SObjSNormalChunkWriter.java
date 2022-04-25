package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.LocationCoder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteNormalChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;

import java.util.concurrent.Executor;

public class SObjSNormalChunkWriter extends SObjSChunkWriter {
    public SObjSNormalChunkWriter(String objKey, InputDataChunkFetcher inputDataFetcher, ChunkObject chunkObj, Executor executor, LocationCoder.CodeType locationType, CSConfiguration csConfig, long startNs, long timeoutSeconds) {
        super(objKey, inputDataFetcher, chunkObj, executor, locationType, csConfig.defaultChunkConfig(), startNs, timeoutSeconds);
    }

    @Override
    protected WriteChunkHandler createWriteChunkHandler(ChunkObject chunkObj, ChunkConfig chunkConfig, Executor executor) {
        return new WriteNormalChunkHandler(chunkObj, this.inputDataFetcher, executor, chunkConfig.csConfig);
    }
}
