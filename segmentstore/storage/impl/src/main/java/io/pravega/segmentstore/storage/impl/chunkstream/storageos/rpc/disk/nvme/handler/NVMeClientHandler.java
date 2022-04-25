package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeShareMemBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class NVMeClientHandler extends NVMeMessageHandler<NVMeResponse> {
    private static final Logger log = LoggerFactory.getLogger(NVMeClientHandler.class);
    private static final Duration NVMeConnectionReceiveResponse = Metrics.makeMetric("NVMeConnection.receiveResponse.duration", Duration.class);
    private final Connection<NVMeMessage> connection;

    public NVMeClientHandler(Connection<NVMeMessage> connection) {
        this.connection = connection;
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected int headerLength() {
        return NVMeMessage.RESPONSE_HEADER_LENGTH;
    }

    @Override
    protected void processMessage(ChannelHandlerContext ctx, NVMeResponse msg) throws IOException {
        submitProcessMessage(ctx, msg);
    }

    @Override
    protected void submitProcessMessage(ChannelHandlerContext ctx, NVMeResponse msg) throws IOException {
        NVMeConnectionReceiveResponse.updateNanoSecond(System.nanoTime() - msg.responseCreateNano());
        connection.complete(msg);
    }

    @Override
    protected NVMeResponse parseMessage(ByteBuf in, int msgSize) throws IOException, CSException {
        var buffer = ByteBuffer.allocate(msgSize);
        in.readBytes(buffer);
        switch (NVMeResponse.messageType(buffer)) {
            case READ:
                return new NVMeReadResponse(buffer);
            case PING:
            case WRITE_ONE_COPY:
                return new NVMeCommonResponse(buffer);
            case HUGE_SHM_INFO_RESPONSE:
                return new NVMeShmFileResponse(buffer);
            case HUGE_SHM_RESPONSE:
                return new NVMeShareMemResponse(buffer);
            case WRITE_THREE_COPIES:
            case TRIM:
            default:
                log.error("unsupported message type {}", NVMeResponse.messageType(buffer));
                return null;
        }
    }

    @Override
    protected NVMeResponse parseMessage(NVMeShareMemBuffer buffer) throws IOException, CSException {
        throw new UnsupportedEncodingException("NVMeClientHandler is not support parseMessage from NVMeShareMemBuffer");
    }
}
