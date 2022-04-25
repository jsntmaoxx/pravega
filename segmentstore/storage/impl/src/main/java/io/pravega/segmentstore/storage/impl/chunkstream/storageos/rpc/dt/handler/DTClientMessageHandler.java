package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class DTClientMessageHandler extends DTMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(DTClientMessageHandler.class);

    private final Connection<FileOperationsPayloads.FileOperationsPayload> connection;

    public DTClientMessageHandler(Executor executor, DTRpcServer rpcServer, Connection<FileOperationsPayloads.FileOperationsPayload> connection) {
        super(executor, rpcServer);
        this.connection = connection;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("{}=>{} encounter exception", ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);
        if (connection != null) {
            connection.completeWithException(cause);
        }
        ctx.close();
    }

    @Override
    protected Logger log() {
        return log;
    }
}
