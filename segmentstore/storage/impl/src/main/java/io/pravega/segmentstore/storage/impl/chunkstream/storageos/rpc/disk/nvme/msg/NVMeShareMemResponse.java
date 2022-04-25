package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Amount;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.ReadResponse;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeShareMemBuffer;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class NVMeShareMemResponse extends NVMeResponse implements ReadResponse {
    private static final Amount ShareMemReadResponseDataAmount = Metrics.makeMetric("NVMeConnection.shm.read.response.data.amount", Amount.class);
    private String requestId;
    private ReadDataBuffer readBuffer;

    public NVMeShareMemResponse(ByteBuffer requestBuffer) {
        super(requestBuffer);
    }

    public void updateShareMemBuffer(NVMeShareMemBuffer shareMemBuffer) throws CSException {
        var headerBuffer = shareMemBuffer.dataPage1().byteBuffer;
        requestId = requestId(headerBuffer, ShareMemResponseOffset);
        var dataSize = dataSize(headerBuffer, ShareMemResponseOffset);
        if (dataSize > 0) {
            ShareMemReadResponseDataAmount.count(dataSize);
            this.readBuffer = ReadDataBuffer.create(dataSize, true);
            shareMemBuffer.resetForRead();
            var offset = NVMeShareMemBuffer.NVMeMessageSize + offsetInData();
            var buf1 = shareMemBuffer.dataPage1().byteBuffer;
            if (buf1.capacity() < offset) {
                offset -= buf1.capacity();
            } else {
                buf1.position(offset);
                offset = 0;
                readBuffer.put(buf1);
            }
            if (readBuffer.writableBytes() > 0) {
                var buf2 = shareMemBuffer.dataPage2().byteBuffer;
                if (offset > 0) {
                    buf2.position(offset);
                }
                readBuffer.put(buf2);
            }
            if (readBuffer.writableBytes() > 0) {
                throw new CSException("share mem is not return all data, still wait on " + readBuffer.writableBytes() + "b");
            }
        } else {
            readBuffer = null;
        }
    }

    @Override
    public String requestId() {
        return requestId;
    }

    public int responseShmBlockId() {
        return shmBlockId();
    }

    @Override
    public int waitDataBytes() {
        return 0;
    }

    @Override
    public void feedData(ByteBuf in) {
        throw new UnsupportedOperationException("NVMeShareMemResponse is not support feedData");
    }

    @Override
    public ReadDataBuffer data() {
        return readBuffer;
    }
}
