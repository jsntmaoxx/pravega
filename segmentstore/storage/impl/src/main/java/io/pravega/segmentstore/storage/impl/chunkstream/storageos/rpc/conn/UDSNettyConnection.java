package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.netty.channel.unix.DomainSocketAddress;

public abstract class UDSNettyConnection<ResponseMessage> extends NettyConnection<ResponseMessage> {
    protected final DomainSocketAddress socketAddress;
    private final String udsPath;

    public UDSNettyConnection(RpcServer<ResponseMessage> rpcServer, String udsPath) {
        super(rpcServer);
        this.udsPath = udsPath;
        this.socketAddress = new DomainSocketAddress(udsPath);
        this.name = "->" + udsPath;
    }

    @Override
    public boolean connect() {
        try {
            this.channel = rpcServer.connect(this.socketAddress, this).sync().channel();
            this.name = channel.id() + "=>" + channel.remoteAddress();
            log().info("create uds connection {}", name);
            return this.channel.isOpen();
        } catch (InterruptedException e) {
            log().error("failed uds connect to {}", name);
        }
        return false;
    }

    @Override
    public int port() {
        return 0;
    }
}
