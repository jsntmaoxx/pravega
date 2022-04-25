package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import com.google.protobuf.ExtensionRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

public abstract class HDDMessageHandler<M extends HDDMessage> extends ByteToMessageDecoder {
    protected static final ExtensionRegistry extensionRegistry;

    static {
        extensionRegistry = ExtensionRegistry.newInstance();
        extensionRegistry.add(StorageServerMessages.writeRequest);
        extensionRegistry.add(StorageServerMessages.readRequest);
        extensionRegistry.add(StorageServerMessages.writeResponse);
        extensionRegistry.add(StorageServerMessages.readResponse);
        extensionRegistry.add(StorageServerMessages.pingRequest);
        extensionRegistry.add(StorageServerMessages.pingResponse);
        extensionRegistry.add(StorageServerMessages.connectionPoolRequest);
    }

    private final Context<M> context = new Context<>();

    private static int readRawVarInt32(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return 0;
        }
        buffer.markReaderIndex();
        byte tmp = buffer.readByte();
        if (tmp >= 0) {
            return tmp;
        } else {
            int result = tmp & 127;
            if (!buffer.isReadable()) {
                buffer.resetReaderIndex();
                return 0;
            }
            if ((tmp = buffer.readByte()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 127) << 7;
                if (!buffer.isReadable()) {
                    buffer.resetReaderIndex();
                    return 0;
                }
                if ((tmp = buffer.readByte()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if (!buffer.isReadable()) {
                        buffer.resetReaderIndex();
                        return 0;
                    }
                    if ((tmp = buffer.readByte()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        if (!buffer.isReadable()) {
                            buffer.resetReaderIndex();
                            return 0;
                        }
                        result |= (tmp = buffer.readByte()) << 28;
                        if (tmp < 0) {
                            throw new CorruptedFrameException("malformed varint.");
                        }
                    }
                }
            }
            return result;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        boolean finish = false;
        while (!finish) {
            switch (context.state) {
                case head:
                    var msgLen = HDDMessageHandler.readRawVarInt32(in);
                    if (msgLen == 0) {
                        if (in.readableBytes() >= CSConfiguration.maxVarInt32Bytes) {
                            log().error("failed to parse var int 32 with buffer {}B", in.readableBytes());
                            throw new CSException("failed to parse var int 32");
                        }
                        finish = true;
                    } else {
                        context.moveToParseCommand(msgLen);
                    }
                    break;
                case command:
                    if (in.readableBytes() >= context.msgSize) {
                        var msg = parseMessage(in, context.msgSize);
                        if (msg.waitDataBytes() > 0) {
                            context.moveToParseData(msg);
                        } else {
                            processMessage(ctx, msg);
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
                    if (context.diskMessage == null) {
                        throw new CSException("no active response receiving data");
                    }
                    context.diskMessage.feedData(in);
                    if (context.diskMessage.waitDataBytes() == 0) {
                        processMessage(ctx, context.diskMessage);
                        context.moveToParseHead();
                    }
                    if (in.readableBytes() == 0) {
                        finish = true;
                    }
                    break;
            }
        }
    }

    protected abstract void processMessage(ChannelHandlerContext ctx, M msg) throws IOException;

    protected abstract M parseMessage(ByteBuf in, int msgSize) throws IOException, CSException;

    protected abstract Logger log();

    @Override
    public boolean isSharable() {
        return false;
    }

    private enum State {
        head,
        command,
        data
    }

    private static class Context<M extends HDDMessage> {
        State state = State.head;
        int msgSize = 0;
        M diskMessage = null;

        public void moveToParseHead() {
            state = State.head;
            msgSize = 0;
            diskMessage = null;
        }

        public void moveToParseCommand(int size) {
            state = State.command;
            this.msgSize = size;
            diskMessage = null;
        }

        public void moveToParseData(M message) {
            state = State.data;
            msgSize = 0;
            diskMessage = message;
        }
    }
}
