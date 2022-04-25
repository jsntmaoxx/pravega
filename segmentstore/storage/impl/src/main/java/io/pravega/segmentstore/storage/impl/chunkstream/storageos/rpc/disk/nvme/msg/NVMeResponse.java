package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class NVMeResponse extends NVMeMessage {
    public static final int ShareMemResponseOffset = 2 * 1024;
    protected final ByteBuffer responseBuffer;
    private final long receiveNano;

    public NVMeResponse(ByteBuffer responseBuffer) {
        this.responseBuffer = responseBuffer;
        this.receiveNano = System.nanoTime();
    }

    public static void writeHeader(ByteBuf response,
                                   MessageType messageType,
                                   String requestId,
                                   int dataSize,
                                   NVMeStatusCode status,
                                   long createTime,
                                   int blockId) {
        response.writeByte(messageType.ordinal());
        continueWriteRequestId(response, requestId);
        response.writeIntLE(dataSize);
        response.writeByte(status.code());
        // skip status code for cp0, cp1 and cp2
        response.writeZero(MSG_STATUS_CODE_LEN * 3);
        response.writeIntLE(MSG_MAGIC_VALUE);
        response.writeLongLE(createTime);
        response.writeZero(MSG_OFFSET_IN_DATA_LEN);
        response.writeIntLE(blockId);
    }

    public static void writeHeader(ByteBuf response, MessageType messageType, NVMeStatusCode status, int blockId) {
        response.writeByte(messageType.ordinal());
        response.writeZero(MSG_REQUEST_ID_LEN + MSG_DATA_SIZE_LEN);
        response.writeByte(status.code());
        // skip status code for cp0, cp1 and cp2
        response.writeZero(MSG_STATUS_CODE_LEN * 3);
        response.writeIntLE(MSG_MAGIC_VALUE);
        response.writeZero(MSG_TIMESTAMP_LEN + MSG_OFFSET_IN_DATA_LEN);
        response.writeIntLE(blockId);
        assert response.writableBytes() == 0;
    }

    public static void continueWriteRequestId(ByteBuf request, String requestId) {
        var requestBytes = requestId.getBytes(StandardCharsets.UTF_8);
        assert requestBytes.length <= MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
        request.writeBytes(requestId.getBytes(StandardCharsets.UTF_8), 0, Math.min(requestBytes.length, MSG_REQUEST_ID_LEN));
        if (requestBytes.length < MSG_REQUEST_ID_LEN) {
            request.writeZero(MSG_REQUEST_ID_LEN - requestBytes.length);
        }
    }

    public static MessageType messageType(ByteBuffer buffer) throws CSException {
        buffer.position(RESPONSE_TYPE_OFFSET);
        return MessageType.from(buffer.get());
    }

    public static String requestId(ByteBuffer buffer, int offset) {
        var buf = new byte[MSG_REQUEST_ID_LEN];
        buffer.position(RESPONSE_REQUEST_ID_OFFSET + offset);
        buffer.get(buf);
        return StringUtils.bytes2String(buf);
    }

    public static long responseCreateNano(ByteBuffer buffer) {
        buffer.position(RESPONSE_TIMESTAMP_OFFSET);
        return Long.reverseBytes(buffer.getLong());
    }

    public static int dataSize(ByteBuffer buffer, int offset) {
        buffer.position(RESPONSE_DATA_SIZE_OFFSET + offset);
        return Integer.reverseBytes(buffer.getInt());
    }

    public long responseCreateNano() {
        return responseCreateNano(responseBuffer);
    }

    public int dataSize() {
        return dataSize(responseBuffer, 0);
    }

    public int offsetInData() {
        responseBuffer.position(RESPONSE_OFFSET_IN_DATA_OFFSET);
        return Integer.reverseBytes(responseBuffer.getInt());
    }

    public int shmBlockId() {
        responseBuffer.position(RESPONSE_SHM_BLOCK_ID_OFFSET);
        return Integer.reverseBytes(responseBuffer.getInt());
    }

    @Override
    public String requestId() {
        return requestId(responseBuffer, 0);
    }

    @Override
    public long requestCreateNano() {
        return 0;
    }

    @Override
    public long responseReceiveNano() {
        return this.receiveNano;
    }

    @Override
    public boolean success() {
        responseBuffer.position(RESPONSE_STATUS_CODE_OFFSET);
        return responseBuffer.get() > 0;
    }

    @Override
    public String errorMessage() {
        return "";
    }
}
