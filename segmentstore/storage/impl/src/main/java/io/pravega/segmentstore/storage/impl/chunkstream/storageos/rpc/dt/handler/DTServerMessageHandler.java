package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.handler;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class DTServerMessageHandler extends DTMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(DTServerMessageHandler.class);

    public DTServerMessageHandler(Executor executor, DTRpcServer rpcServer) {
        super(executor, rpcServer);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("{}=>{} encounter exception", ctx.channel().remoteAddress(), ctx.channel().localAddress(), cause);
        ctx.close();
    }
}
