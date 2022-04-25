package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.ConnectionGroup;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.handler.NVMeClientHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

public class NVMeRpcClientServer extends DiskRpcClientServer<NVMeMessage> {
    private static final Logger log = LoggerFactory.getLogger(NVMeRpcClientServer.class);

    protected final EpollEventLoopGroup workerGroup;
    protected final Bootstrap bootstrap;
    protected final NVMeRpcConfiguration nvmeRpcConfig;
    private final String udsPath;
    private final String udsShareMemPath;
    protected volatile ConnectionGroup<NVMeMessage> localConnGroup;

    public NVMeRpcClientServer(NVMeRpcConfiguration nvmeRpcConfig, String udsPath, String udsShareMemPath) {
        this.nvmeRpcConfig = nvmeRpcConfig;
        this.udsPath = udsPath;
        this.udsShareMemPath = udsShareMemPath;
        workerGroup = new EpollEventLoopGroup(nvmeRpcConfig.nettyServiceEventLoopNumber());
        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(EpollDomainSocketChannel.class);
    }

    @Override
    protected ConnectionGroup<NVMeMessage> connectionGroup(String nodeIp, int port) {
        throw new UnsupportedOperationException("not support connectionGroup in NVMeRpcClientServer");
    }

    @Override
    protected ConnectionGroup<NVMeMessage> localConnectionGroup() {
        if (localConnGroup == null) {
            synchronized (this) {
                if (localConnGroup != null) {
                    return localConnGroup;
                }
                var group = new NVMeConnectionGroup(this,
                                                    nvmeRpcConfig.enableSharedMemConnection() ? udsShareMemPath : udsPath,
                                                    nvmeRpcConfig.connectionNumber(),
                                                    nvmeRpcConfig.enableSharedMemConnection());
                group.connect();
                localConnGroup = group;
            }
        }
        return localConnGroup;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (localConnGroup != null) {
            localConnGroup.shutdown();
        }
        try {
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            log.warn("nvme client server graceful shutdown was interrupted", e);
        }
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, Connection<NVMeMessage> connection) {
        var bs = bootstrap.clone();
        bs.handler(new ChannelInitializer<DomainSocketChannel>() {
            @Override
            protected void initChannel(DomainSocketChannel ch) throws Exception {
                ch.pipeline().addLast(new NVMeClientHandler(connection));
            }
        });
        return bs.connect(socketAddress);
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public RpcConfiguration rpcConfig() {
        return this.nvmeRpcConfig;
    }
}
