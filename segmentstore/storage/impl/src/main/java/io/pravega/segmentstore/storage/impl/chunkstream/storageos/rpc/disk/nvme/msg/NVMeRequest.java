package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeShareMemBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class NVMeRequest extends NVMeMessage {

    protected final ByteBuffer requestBuffer;
    private final long receiveNano;

    public NVMeRequest(ByteBuffer requestBuffer) {
        this.requestBuffer = requestBuffer;
        this.receiveNano = System.nanoTime();
    }

    public static MessageType messageType(ByteBuffer buffer) throws CSException {
        buffer.position(REQUEST_TYPE_OFFSET);
        return MessageType.from(buffer.get());
    }

    public static String requestId(ByteBuffer buffer) {
        var buf = new byte[MSG_REQUEST_ID_LEN];
        buffer.position(REQUEST_REQUEST_ID_OFFSET);
        buffer.get(buf);
        return StringUtils.bytes2String(buf);
    }

    public static void writeHeader(ByteBuf request, MessageType messageType, String requestId, int dataSize, int nodeIp, String partition, long diskOffset, long createTime) {
        request.writeByte(messageType.ordinal());
        continueWriteRequestId(request, requestId);
        request.writeIntLE(dataSize);
        request.writeLongLE(diskOffset);
        continueWriteSeg0PartitionId(request, partition);
        request.writeIntLE(nodeIp);
        // skip seg1 and seg2
        request.writeZero(MSG_SEGMENT_INFO_LEN + MSG_SEGMENT_INFO_LEN);
        request.writeIntLE(MSG_MAGIC_VALUE);
        request.writeLongLE(createTime);
        assert request.writableBytes() == 0;
    }

    public static void writeHeader(ByteBuf request, MessageType messageType, String requestId, long createTime) {
        request.writeByte(messageType.ordinal());
        continueWriteRequestId(request, requestId);
        request.writeZero(MSG_DATA_SIZE_LEN + MSG_SEGMENT_INFO_LEN * 3);
        request.writeIntLE(MSG_MAGIC_VALUE);
        request.writeLongLE(createTime);
        assert request.writableBytes() == 0;
    }

    public static void continueWriteRequestId(ByteBuf request, String requestId) {
        var requestBytes = requestId.getBytes(StandardCharsets.UTF_8);
        assert requestBytes.length <= MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
        request.writeBytes(requestId.getBytes(StandardCharsets.UTF_8), 0, Math.min(requestBytes.length, MSG_REQUEST_ID_LEN));
        if (requestBytes.length < MSG_REQUEST_ID_LEN) {
            request.writeZero(MSG_REQUEST_ID_LEN - requestBytes.length);
        }
    }

    private static void continueWriteSeg0PartitionId(ByteBuf request, String partition) {
        var buf = partition.getBytes(StandardCharsets.UTF_8);
        assert buf.length <= MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
        request.writeBytes(buf, 0, Math.min(buf.length, MSG_SEGMENT_INFO_PARTITION_UUID_LEN));
        if (buf.length < MSG_SEGMENT_INFO_PARTITION_UUID_LEN) {
            request.writeZero(MSG_SEGMENT_INFO_PARTITION_UUID_LEN - buf.length);
        }
    }

    public MessageType messageType() throws CSException {
        return messageType(requestBuffer);
    }

    public int seg0NodeIp() {
        requestBuffer.position(REQUEST_SEGMENT_INFO_0_NODE_IP_OFFSET);
        return Integer.reverseBytes(requestBuffer.getInt());
    }

    public String seg0PartitionId() {
        var buf = new byte[MSG_SEGMENT_INFO_PARTITION_UUID_LEN];
        requestBuffer.position(REQUEST_SEGMENT_INFO_0_PARTITION_UUID_OFFSET);
        requestBuffer.get(buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    public long seg0PartitionOffset() {
        requestBuffer.position(REQUEST_SEGMENT_INFO_0_OFFSET);
        return Long.reverseBytes(requestBuffer.getLong());
    }

    public int dataSize() {
        requestBuffer.position(REQUEST_DATA_SIZE_OFFSET);
        return Integer.reverseBytes(requestBuffer.getInt());
    }

    @Override
    public String requestId() {
        return requestId(requestBuffer);
    }

    @Override
    public long requestCreateNano() {
        requestBuffer.position(REQUEST_TIMESTAMP_OFFSET);
        return Long.reverseBytes(requestBuffer.getLong());
    }

    @Override
    public long responseReceiveNano() {
        return receiveNano;
    }

    @Override
    public boolean success() {
        throw new UnsupportedOperationException("NVMeRequest is not support success");
    }

    @Override
    public String errorMessage() {
        throw new UnsupportedOperationException("NVMeRequest is not support errorMessage");
    }

    public abstract ByteBuf process(ChannelHandlerContext ctx);

    public abstract ByteBuf sendBuffer();

    public abstract NVMeShareMemBuffer shmBuffer();
}
