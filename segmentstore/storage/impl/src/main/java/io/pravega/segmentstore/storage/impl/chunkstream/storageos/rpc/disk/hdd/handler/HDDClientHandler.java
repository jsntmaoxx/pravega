package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Duration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.Response;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDCommonResponse;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDReadResponse;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDResponse;
import com.google.protobuf.CodedInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.StorageServerMessages.readResponse;

public class HDDClientHandler extends HDDMessageHandler<HDDResponse> {
    private static final Logger log = LoggerFactory.getLogger(HDDClientHandler.class);
    private static final Duration HDDConnectionReceiveResponse = Metrics.makeMetric("HDDConnection.receiveResponse.duration", Duration.class);
    protected final Connection<HDDMessage> connection;

    public HDDClientHandler(Connection<HDDMessage> connection) {
        this.connection = connection;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("{}=>{} encounter exception", ctx.channel().remoteAddress(), ctx.channel().localAddress(), cause);
        connection.completeWithException(cause);
        ctx.close();
    }

    @Override
    protected void processMessage(ChannelHandlerContext ctx, HDDResponse msg) {
        HDDConnectionReceiveResponse.updateNanoSecond(System.nanoTime() - msg.responseCreateNano());
        connection.complete(msg);
    }

    @Override
    protected HDDResponse parseMessage(ByteBuf in, int size) throws IOException {
        Response response;
        if (in.hasArray()) {
            response = Response.parseFrom(CodedInputStream.newInstance(in.array(), in.arrayOffset() + in.readerIndex(), size));
        } else {
            response = Response.parseFrom(new ByteBufInputStream(in.slice(in.readerIndex(), size)), extensionRegistry);
        }
        in.skipBytes(size);
        if (response.hasExtension(readResponse)) {
            return new HDDReadResponse(response, (int) response.getExtension(readResponse).getDataLength());
        } else {
            return new HDDCommonResponse(response);
        }
    }

    @Override
    protected Logger log() {
        return log;
    }
}
