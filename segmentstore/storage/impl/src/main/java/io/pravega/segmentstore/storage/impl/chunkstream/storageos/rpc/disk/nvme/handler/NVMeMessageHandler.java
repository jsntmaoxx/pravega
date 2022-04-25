package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.NVMeShareMemBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

public abstract class NVMeMessageHandler<M extends NVMeMessage> extends ByteToMessageDecoder {

    private final Context<M> context = new Context<>();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        boolean finish = false;
        var headerLength = headerLength();
        while (!finish) {
            switch (context.state) {
                case head:
                    if (in.readableBytes() >= headerLength) {
                        M msg;
                        try {
                            msg = parseMessage(in, headerLength);
                            if (msg == null) {
                                ctx.close();
                                return;
                            }
                        } catch (Exception e) {
                            log().error("parse message failed, close connection", e);
                            ctx.close();
                            return;
                        }
                        if (msg.waitDataBytes() > 0) {
                            context.moveToParseData(msg);
                        } else {
                            submitProcessMessage(ctx, msg);
                            context.moveToParseHead();
                        }
                        if (in.readableBytes() == 0) {
                            finish = true;
                        }
                    } else {
                        finish = true;
                    }
                    break;
                case data:
                    if (context.nvmeMessage == null) {
                        throw new CSException("no active response receiving data");
                    }
                    context.nvmeMessage.feedData(in);
                    if (context.nvmeMessage.waitDataBytes() == 0) {
                        submitProcessMessage(ctx, context.nvmeMessage);
                        context.moveToParseHead();
                    }
                    if (in.readableBytes() == 0) {
                        finish = true;
                    }
                    break;
            }
        }
    }

    protected abstract Logger log();

    protected abstract int headerLength();

    protected abstract void processMessage(ChannelHandlerContext ctx, M msg) throws IOException;

    protected abstract void submitProcessMessage(ChannelHandlerContext ctx, M msg) throws IOException;

    protected abstract M parseMessage(ByteBuf in, int msgSize) throws IOException, CSException;

    protected abstract M parseMessage(NVMeShareMemBuffer buffer) throws IOException, CSException;

    private enum State {
        head,
        data
    }

    private static class Context<M extends NVMeMessage> {
        State state = State.head;
        M nvmeMessage = null;

        public void moveToParseHead() {
            state = State.head;
            nvmeMessage = null;
        }

        public void moveToParseData(M message) {
            state = State.data;
            nvmeMessage = message;
        }
    }
}
