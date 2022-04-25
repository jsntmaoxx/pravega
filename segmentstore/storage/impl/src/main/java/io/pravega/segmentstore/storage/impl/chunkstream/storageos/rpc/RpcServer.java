package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;

import java.net.SocketAddress;

public interface RpcServer<ResponseMessage> {
    void start() throws InterruptedException;

    void join() throws InterruptedException;

    void shutdown();

    RpcConfiguration rpcConfig();

    ChannelFuture connect(SocketAddress socketAddress, Connection<ResponseMessage> connection);

    Logger log();
}
