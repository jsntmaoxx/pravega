package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;

import java.net.InetSocketAddress;

public abstract class IPNettyConnection<ResponseMessage> extends NettyConnection<ResponseMessage> {
    protected final String host;
    protected final int port;
    protected final InetSocketAddress socketAddress;

    public IPNettyConnection(RpcServer<ResponseMessage> rpcServer, String host, int port) {
        super(rpcServer);
        this.host = host;
        this.port = port;
        this.name = "->" + host + ":" + port;
        this.socketAddress = InetSocketAddress.createUnresolved(this.host, this.port);
    }

    @Override
    public boolean connect() {
        try {
            this.channel = rpcServer.connect(this.socketAddress, this).sync().channel();
            // in large write, it can reduce response receive time (20%), but no help to overall latency
//            var config = (SocketChannelConfig)this.channel.config();
//            config.setWriteSpinCount(1);
            this.name = channel.localAddress() + "=>" + channel.remoteAddress();
            log().info("create connection {}", name);
            return this.channel.isOpen();
        } catch (InterruptedException e) {
            log().error("failed connect to {}", name);
        } catch (Throwable e) {
            log().error("failed connect, ex", e);
        }
        return false;
    }

    @Override
    public int port() {
        return port;
    }
}
