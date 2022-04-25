package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTMessageBuilder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import com.google.protobuf.CodedInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.CommandType.REQUEST_PING;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTMessageBuilder.FooterLength;

public abstract class DTMessageHandler extends ByteToMessageDecoder {

    protected final Executor executor;
    protected final DTRpcServer rpcServer;
    private State state = State.Header;
    private long sendTimeMs;
    private int messageSize;
    private long checksum;

    public DTMessageHandler(Executor executor, DTRpcServer rpcServer) {
        this.executor = executor;
        this.rpcServer = rpcServer;
    }

    protected void onRequest(FileOperationsPayload request) {
        switch (request.getCommandType()) {
            case REQUEST_PING:
                executor.execute(() -> onPing(request));
                break;
            default:
                log().warn("unhandled request {} {}", request.getCommandType(), request.getRequestId());
                break;
        }
    }

    protected void onResponse(FileOperationsPayload response) {
        rpcServer.complete(response);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        boolean finish = false;
        while (!finish) {
            switch (state) {
                case Header:
                    if (in.readableBytes() < DTMessageBuilder.HeaderLength) {
                        finish = true;
                        break;
                    }
                    sendTimeMs = in.readLong();
                    messageSize = in.readInt();
                    state = State.Message;
                    break;
                case Message:
                    if (in.readableBytes() < messageSize) {
                        finish = true;
                        break;
                    }
                    var msg = parseMessage(in, messageSize);
                    dispatchMessage(msg, sendTimeMs, System.nanoTime());
                    state = State.Header;
                    break;
                default:
                    throw new CSException("unknown parser status");
            }
        }
    }

    protected void dispatchMessage(FileOperationsPayload msg, long sendTimeMillion, long receiveNano) {
        var commandType = msg.getCommandType();
        if ((commandType.getNumber() & 0x1) == 1) {
            onResponse(msg);
            return;
        }
        onRequest(msg);
    }

    private FileOperationsPayload parseMessage(ByteBuf in, int size) throws IOException {
        FileOperationsPayload msg;
        size -= FooterLength;
        if (in.hasArray()) {
            msg = FileOperationsPayload.parseFrom(CodedInputStream.newInstance(in.array(), in.arrayOffset() + in.readerIndex(), size));
        } else {
            msg = FileOperationsPayload.parseFrom(new ByteBufInputStream(in.slice(in.readerIndex(), size)));
        }
        in.skipBytes(size);
        checksum = in.readLong();
        return msg;
    }

    protected abstract Logger log();

    private void onPing(FileOperationsPayload request) {
        try {
            var createNano = System.nanoTime();
            rpcServer.sendResponse(request.getRequestId(),
                                   request.getCallerId(),
                                   request.getResponsePort(),
                                   createNano,
                                   DTMessageBuilder.makeByteBuf(DTMessageBuilder.makePingResponse(request), createNano));
        } catch (Exception e) {
            log().error("handle {} {} caller {}:{} failed", REQUEST_PING, request.getRequestId(), request.getCallerId(), request.getResponsePort());
        }
    }

    private enum State {
        Header,
        Message,
    }
}
