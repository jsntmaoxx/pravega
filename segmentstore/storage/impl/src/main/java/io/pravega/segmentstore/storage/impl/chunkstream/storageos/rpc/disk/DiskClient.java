package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public interface DiskClient<ResponseMessage> {
    CompletableFuture<? extends ResponseMessage> write(String device,
                                                       String requestId,
                                                       String partition,
                                                       String bin,
                                                       long offset,
                                                       WriteBatchBuffers batchBuffers,
                                                       boolean sync) throws Exception;

    CompletableFuture<? extends ResponseMessage> nativeWrite(String device,
                                                             String requestId,
                                                             String partition,
                                                             String bin,
                                                             long offset,
                                                             ByteBuf codeBuf) throws Exception;

    CompletableFuture<? extends ResponseMessage> read(String device,
                                                      String requestId,
                                                      String partition,
                                                      String bin,
                                                      long offset,
                                                      int length) throws Exception;
}
