package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.SharedMemory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeShareMemBuffer.NVMeMessageSize;

public class NVMeShareMemConnection extends NVMeConnection {
    private static final Logger log = LoggerFactory.getLogger(NVMeShareMemConnection.class);
    private static final Count NVMeShmConnAcquireBufferCount = Metrics.makeMetric("NVMeShmConn.acquire.shm.count", Count.class);
    private static final Count NVMeShmConnReleaseBufferCount = Metrics.makeMetric("NVMeShmConn.release.shm.count", Count.class);
    private final RpcConfiguration rpcConfig;
    private List<NVMeShareMemBuffer> shmBuffers;

    public NVMeShareMemConnection(RpcServer<NVMeMessage> rpcServer, String udsPath, RpcConfiguration rpcConfig) {
        super(rpcServer, udsPath);
        this.rpcConfig = rpcConfig;
    }

    @Override
    public boolean connect() {
        var success = super.connect();
        if (!success) {
            return false;
        }
        var createTime = System.nanoTime();
        var request = Unpooled.directBuffer(NVMeMessage.REQUEST_HEADER_LENGTH, NVMeMessage.REQUEST_HEADER_LENGTH);
        var requestId = "r-shm-" + G.genRequestId();
        NVMeRequest.writeHeader(request, NVMeMessage.MessageType.HUGE_SHM_INFO_REQUEST, requestId, createTime);
        try {
            var message = (NVMeShmFileResponse) super.sendRequest(requestId, createTime, request, null).get(rpcConfig.rpcTimeoutSeconds(), TimeUnit.SECONDS);
            updateShareMemInfo(message.shareMemInfo());
            return true;
        } catch (Exception e) {
            log.error("get share memory info failed", e);
        }
        return false;
    }

    @Override
    public boolean isActive() {
        return super.isActive() && this.shmBuffers != null;
    }

    @Override
    public boolean isOpen() {
        return super.isOpen() && this.shmBuffers != null;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        unmapShmPages();
        log().info("unmap share memory on connection {}", name);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    public CompletableFuture<NVMeMessage> sendRequest(String requestId, long createTime, ByteBuf buf) {
        return sendRequest(requestId, createTime, buf, null);
    }

    @Override
    public CompletableFuture<NVMeMessage> sendRequest(String requestId, long createTime, ByteBuf requestBuf, ByteBuf payloadBuf) {
        var sendSize = requestBuf.readableBytes() + (payloadBuf == null ? 0 : payloadBuf.readableBytes());
        if (sendSize < SharedMemory.PageSize * 2) {
            for (var shmBuffer : shmBuffers) {
                if (!shmBuffer.hasResponseFlag() && shmBuffer.lock()) {
                    NVMeShmConnAcquireBufferCount.increase();
                    var f = sendRequestToShareMem(shmBuffer, requestId, createTime, requestBuf, payloadBuf);
                    shmBuffer.lockRequestFlag();
                    return f;
                }
            }
            log().warn("{} send request {} by uds due to can not find free share mem buffer", name, requestId);
        } else {
            log().warn("{} send request {} by uds due to buffer size {} large than share mem size {}",
                       name, requestId, sendSize, SharedMemory.PageSize * 2);
        }
        return super.sendRequest(requestId, createTime, requestBuf, payloadBuf);
    }

    @Override
    public void complete(NVMeMessage message) {
        try {
            if (message instanceof NVMeShareMemResponse) {
                var shmResponse = (NVMeShareMemResponse) message;
                var shmBlockId = shmResponse.responseShmBlockId();
                if (shmBlockId >= 0 && shmBuffers != null) {
                    var shmBuffer = shmBuffers.get(shmBlockId);
                    shmResponse.updateShareMemBuffer(shmBuffer);
                    assert shmBuffer.hasRequestFlag() && shmBuffer.hasResponseFlag();
                    shmBuffer.unlockRequestFlag();
                    shmBuffer.unlock();
                    NVMeShmConnReleaseBufferCount.increase();
                }
            }
            super.complete(message);
        } catch (Exception e) {
            super.completeWithException(e);
        }
    }

    private CompletableFuture<NVMeMessage> sendRequestToShareMem(NVMeShareMemBuffer shmBuffer,
                                                                 String requestId,
                                                                 long createTime,
                                                                 ByteBuf requestBuf,
                                                                 ByteBuf payloadBuf) {
        shmBuffer.resetForWrite();
        var retFuture = new CompletableFuture<NVMeMessage>();
        if (channel == null || !channel.isOpen()) {
            log().error("can not connect to {}", name());
            retFuture.completeExceptionally(new CSException("can not connect to " + name()));
            return retFuture;
        }
        if (!storeRequest(requestId, retFuture)) {
            retFuture.completeExceptionally(new CSException("failed store request " + requestId));
            return retFuture;
        }
        try {
            shmBuffer.write(requestBuf);
            requestBuf.release();
            shmBuffer.dataPage1().byteBuf.writerIndex(NVMeMessageSize);
            if (payloadBuf != null && !rpcConfig.skipShareMemDataCopy()) {
                shmBuffer.write(payloadBuf);
                payloadBuf.release();
            }
            markSendComplete(createTime);
        } catch (Exception e) {
            log().error("send request {} to share mem failed", requestId, e);
            retFuture.completeExceptionally(new CSException("send request " + requestId + " to share mem failed", e));
        }
        return retFuture;
    }

    private void updateShareMemInfo(ShareMemInfo shareMemInfo) throws CSException {
        if (shmBuffers != null) {
            log().error("have had map share memory pages, release it");
            unmapShmPages();
        }
        if (shareMemInfo.dataPageCount % 2 != 0) {
            log().error("data page count " + shareMemInfo.dataPageCount + " is odd, require a even number %2 == 0");
            unmapShmPages();
            throw new CSException("data page count " + shareMemInfo.dataPageCount + " is odd, require a even number");
        }
        var shmBufferCount = shareMemInfo.dataPageCount / 2;
        try {
            shmBuffers = new ArrayList<>(shmBufferCount);
            var ctlPage = SharedMemory.map(shareMemInfo.ctlFileInfo.fileName, shareMemInfo.ctlFileInfo.size);
            var subIndexPos = shareMemInfo.dataFileInfo.fileName.lastIndexOf(SharedMemory.subIndexSplit);
            if (subIndexPos == -1) {
                throw new CSException("illegal share mem file name " + shareMemInfo.dataFileInfo.fileName);
            }
            var dataFilePrefix = shareMemInfo.dataFileInfo.fileName.substring(0, subIndexPos + 1);
            var subIndex = Integer.parseInt(shareMemInfo.dataFileInfo.fileName.substring(subIndexPos + 1));
            for (var i = 0; i < shmBufferCount; ++i) {
                var dp1 = SharedMemory.map(dataFilePrefix + subIndex++, shareMemInfo.dataFileInfo.size);
                var dp2 = SharedMemory.map(dataFilePrefix + subIndex++, shareMemInfo.dataFileInfo.size);
                shmBuffers.add(new NVMeShareMemBuffer(ctlPage, shareMemInfo.slotId * 8, i, dp1, dp2));
            }
            log().info("map share memory on connection {} ctl page {} data page {}[0-{}]", name, shareMemInfo.ctlFileInfo.fileName, dataFilePrefix, shareMemInfo.dataPageCount);
        } catch (Exception e) {
            log().error("map share memory on connection {} failed", name, e);
            unmapShmPages();
            throw e;
        }
    }

    private void unmapShmPages() {
        if (shmBuffers != null) {
            if (!shmBuffers.isEmpty()) {
                SharedMemory.unmap(shmBuffers.get(0).ctlPage());
            }
            for (var p : shmBuffers) {
                SharedMemory.unmap(p.dataPage1());
                SharedMemory.unmap(p.dataPage2());
            }
            shmBuffers = null;
        }
    }
}
