package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.ReadRequest;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Request;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.WriteRequest;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.readRequest;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.writeRequest;

public class HDDClient implements DiskClient<HDDMessage> {
    private final DiskRpcClientServer<HDDMessage> rpcServer;
    private final CSConfiguration csConfig;
    private final Cluster cluster;

    public HDDClient(DiskRpcClientServer<HDDMessage> rpcServer, CSConfiguration csConfig, Cluster cluster) {
        this.rpcServer = rpcServer;
        this.csConfig = csConfig;
        this.cluster = cluster;
    }

    @Override
    public CompletableFuture<? extends HDDMessage> write(String device,
                                                         String requestId,
                                                         String partition,
                                                         String bin,
                                                         long offset,
                                                         WriteBatchBuffers batchBuffers,
                                                         boolean sync) throws Exception {
        var createTime = System.nanoTime();
        Request request = makeWriteRequest(requestId, partition, bin, offset, batchBuffers.size(), createTime, sync);

        var buf = makeByteBuf(request, batchBuffers);
        return rpcServer.sendRequest(cluster.deviceIdToDataIp(device), Cluster.ssDataPort, requestId, createTime, buf);
    }

    @Override
    public CompletableFuture<? extends HDDMessage> nativeWrite(String device,
                                                               String requestId,
                                                               String partition,
                                                               String bin,
                                                               long offset,
                                                               ByteBuf codeBuf) throws Exception {
        var createTime = System.nanoTime();
        Request request = makeWriteRequest(requestId, partition, bin, offset, codeBuf.readableBytes(), createTime, true);

        var buf = makeByteBuf(request, codeBuf);
        return rpcServer.sendRequest(cluster.deviceIdToDataIp(device), Cluster.ssDataPort, requestId, createTime, buf);
    }

    @Override
    public CompletableFuture<? extends HDDMessage> read(String device,
                                                        String requestId,
                                                        String partition,
                                                        String bin,
                                                        long offset,
                                                        int length) throws Exception {
        var createTime = System.nanoTime();
        Request request = makeReadRequest(requestId, partition, bin, offset, length, createTime);

        var buf = makeByteBuf(request);
        return rpcServer.sendRequest(cluster.deviceIdToDataIp(device), Cluster.ssDataPort, requestId, createTime, buf);
    }

    private Request makeWriteRequest(String requestId, String partition, String bin, long offset, int size, long createTime, boolean b) {
        return Request.newBuilder()
                      .setRequestId(requestId)
                      .setTimeout(csConfig.writeDiskTimeoutSeconds() * 1000L)
                      .setVersion(HDDMessage.VERSION)
                      .setMessageCheckSum(HDDMessage.DEFAULT_CHECKSUM)
                      .setCreateTime(createTime)
                      .setExtension(writeRequest, WriteRequest.newBuilder()
                                                              .setPartitionUuid(partition)
                                                              .setFileName(bin)
                                                              .setStartingOffset(offset)
                                                              .setDataLength(size)
                                                              .setSyncOnWrite(b)
                                                              .build())
                      .build();
    }

    private Request makeReadRequest(String requestId, String partition, String bin, long offset, int length, long createTime) {
        return Request.newBuilder()
                      .setRequestId(requestId)
                      .setTimeout(csConfig.readDiskTimeoutSeconds() * 1000L)
                      .setVersion(HDDMessage.VERSION)
                      .setMessageCheckSum(HDDMessage.DEFAULT_CHECKSUM)
                      .setCreateTime(createTime)
                      .setExtension(readRequest, ReadRequest.newBuilder()
                                                            .setPartitionUuid(partition)
                                                            .setFileName(bin)
                                                            .setStartingOffset(offset)
                                                            .setDataLength(length)
                                                            .build())
                      .build();
    }

    private ByteBuf makeByteBuf(Request request) throws IOException {
        var cmdBuf = Unpooled.directBuffer(request.getSerializedSize() + CSConfiguration.maxVarInt32Bytes);
        request.writeDelimitedTo(new ByteBufOutputStream(cmdBuf));
        return cmdBuf;
    }

    private ByteBuf makeByteBuf(Request request, WriteBatchBuffers batchBuffers) throws IOException {
        var cmdBuf = Unpooled.directBuffer(request.getSerializedSize() + CSConfiguration.maxVarInt32Bytes);
        request.writeDelimitedTo(new ByteBufOutputStream(cmdBuf));
        var bufferList = batchBuffers.buffers();
        var buffers = new ByteBuf[1 + bufferList.size()];
        buffers[0] = cmdBuf;
        var i = 1;
        for (var b : bufferList) {
            buffers[i++] = b.wrapToByteBuf();
        }
        return Unpooled.wrappedBuffer(buffers);
    }

    private ByteBuf makeByteBuf(Request request, ByteBuf codeBuf) throws IOException {
        var cmdBuf = Unpooled.directBuffer(request.getSerializedSize() + CSConfiguration.maxVarInt32Bytes);
        request.writeDelimitedTo(new ByteBufOutputStream(cmdBuf));
        var buffers = new ByteBuf[]{cmdBuf, codeBuf};
        return Unpooled.wrappedBuffer(buffers);
    }

}
