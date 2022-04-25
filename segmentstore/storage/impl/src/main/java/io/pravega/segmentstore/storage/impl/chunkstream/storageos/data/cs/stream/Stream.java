package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Stream extends AutoCloseable {
    CompletableFuture<Boolean> open(String streamId, boolean create) throws Exception;

    // size of data, limited to 1M or 2M, won't larger than chunk size
    CompletableFuture<Long> append(ByteBuffer data) throws CSException;

    CompletableFuture<Void> truncate(long streamOffset);

    // cj_todo
    // iterate style / support JDK stream lib?  will be done in higher level Util class.
    CompletableFuture<Pair<Long, ByteBuffer>> read(long streamOffset);

    CompletableFuture<List<Pair<Long, ByteBuffer>>> read(long streamOffset, int maxBytes);

    //config()
}
