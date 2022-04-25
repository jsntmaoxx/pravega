package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteBatchBuffers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage.REQUEST_HEADER_LENGTH;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage.RESPONSE_HEADER_LENGTH;

public class NVMeClient implements DiskClient<NVMeMessage> {
    private final DiskRpcClientServer<NVMeMessage> rpcServer;
    private final CSConfiguration csConfig;
    private final Cluster cluster;

    public NVMeClient(DiskRpcClientServer<NVMeMessage> rpcServer, CSConfiguration csConfig, Cluster cluster) {
        this.rpcServer = rpcServer;
        this.csConfig = csConfig;
        this.cluster = cluster;
    }

    @Override
    public CompletableFuture<? extends NVMeMessage> write(String device, String requestId, String partition, String bin, long offset, WriteBatchBuffers batchBuffers, boolean sync) throws Exception {
        if (csConfig.skipNVMeRepoWrite()) {
            var f = new CompletableFuture<NVMeMessage>();
            var response = ByteBuffer.allocate(RESPONSE_HEADER_LENGTH);
            NVMeResponse.writeHeader(Unpooled.wrappedBuffer(response).clear(), NVMeMessage.MessageType.WRITE_ONE_COPY, requestId, 0, NVMeStatusCode.SUCCESS, System.nanoTime(), -1);
            f.complete(new NVMeCommonResponse(response));
            return f;
        }

        var createTime = System.nanoTime();
        var request = Unpooled.directBuffer(REQUEST_HEADER_LENGTH, REQUEST_HEADER_LENGTH);
        var diskOffset = Cluster.BinSize * Long.parseLong(bin) + offset;
        NVMeRequest.writeHeader(request, NVMeMessage.MessageType.WRITE_ONE_COPY, requestId, batchBuffers.size(), cluster.deviceIdToRdmaIntIp(device), partition, diskOffset, createTime);
        request.writerIndex(REQUEST_HEADER_LENGTH);
        var payloadBuf = makeByteBuf(batchBuffers);
        return rpcServer.sendLocalRequest(requestId, createTime, request, payloadBuf);
    }

    @Override
    public CompletableFuture<? extends NVMeMessage> nativeWrite(String device, String requestId, String partition, String bin, long offset, ByteBuf codeBuf) throws Exception {
        var createTime = System.nanoTime();
        var request = Unpooled.directBuffer(REQUEST_HEADER_LENGTH, REQUEST_HEADER_LENGTH);
        var diskOffset = Cluster.BinSize * Long.parseLong(bin) + offset;
        NVMeRequest.writeHeader(request, NVMeMessage.MessageType.WRITE_ONE_COPY, requestId, codeBuf.readableBytes(), cluster.deviceIdToRdmaIntIp(device), partition, diskOffset, createTime);
        request.writerIndex(REQUEST_HEADER_LENGTH);
        return rpcServer.sendLocalRequest(requestId, createTime, request, codeBuf);
    }

    @Override
    public CompletableFuture<? extends NVMeMessage> read(String device, String requestId, String partition, String bin, long offset, int length) throws Exception {
        var createTime = System.nanoTime();
        var request = Unpooled.directBuffer(REQUEST_HEADER_LENGTH, REQUEST_HEADER_LENGTH);
        var diskOffset = Cluster.BinSize * Long.parseLong(bin) + offset;
        NVMeRequest.writeHeader(request, NVMeMessage.MessageType.READ, requestId, length, cluster.deviceIdToRdmaIntIp(device), partition, diskOffset, createTime);
        return rpcServer.sendLocalRequest(requestId, createTime, request, null);
    }

    private ByteBuf makeByteBuf(ByteBuf requestBuf, WriteBatchBuffers batchBuffers) {
        var bufferList = batchBuffers.buffers();
        assert !bufferList.isEmpty();
        var buffers = new ByteBuf[1 + bufferList.size()];
        buffers[0] = requestBuf;
        var i = 1;
        for (var b : bufferList) {
            buffers[i++] = b.wrapToByteBuf();
        }
        return Unpooled.wrappedBuffer(buffers);
    }

    private ByteBuf makeByteBuf(WriteBatchBuffers batchBuffers) {
        var bufferList = batchBuffers.buffers();
        var count = bufferList.size();
        assert count > 0;
        if (count == 1) {
            return bufferList.get(0).wrapToByteBuf();
        }

        var buffers = new ByteBuf[count];
        var i = 0;
        for (var b : bufferList) {
            buffers[i++] = b.wrapToByteBuf();
        }
        return Unpooled.wrappedBuffer(buffers);
    }

    private ByteBuf makeByteBuf(ByteBuf requestBuf, ByteBuf codeBuf) throws IOException {
        var buffers = new ByteBuf[]{requestBuf, codeBuf};
        return Unpooled.wrappedBuffer(buffers);
    }
}
