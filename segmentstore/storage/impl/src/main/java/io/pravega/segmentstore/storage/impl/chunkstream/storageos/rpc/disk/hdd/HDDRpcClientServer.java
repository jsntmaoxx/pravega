package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.ConnectionGroup;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.handler.HDDClientHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class HDDRpcClientServer extends DiskRpcClientServer<HDDMessage> {
    private static final Logger log = LoggerFactory.getLogger(HDDRpcClientServer.class);
    protected final NioEventLoopGroup workerGroup;
    protected final Bootstrap bootstrap;
    protected final HDDRpcConfiguration hddRpcConfig;

    public HDDRpcClientServer(HDDRpcConfiguration hddRpcConfig) {
        this.hddRpcConfig = hddRpcConfig;
        workerGroup = new NioEventLoopGroup(hddRpcConfig.nettyServiceEventLoopNumber());
        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        workerGroup.shutdownGracefully();
    }

    @Override
    public ChannelFuture connect(SocketAddress socketAddress, Connection<HDDMessage> connection) {
        var bs = bootstrap.clone();
        bs.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new HDDClientHandler(connection));
            }
        });
        return bs.connect(socketAddress);
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    protected ConnectionGroup<HDDMessage> connectionGroup(String nodeIp, int port) {
        var subMap = deviceConnGroupMap.computeIfAbsent(nodeIp, (ni) -> new ConcurrentHashMap<>());
        return subMap.computeIfAbsent(port, (p) -> {
            var group = new HDDConnectionGroup(this, nodeIp, port, hddRpcConfig.connectionNumber());
            group.connect();
            return group;
        });
    }

    @Override
    protected ConnectionGroup<HDDMessage> localConnectionGroup() {
        throw new UnsupportedOperationException("not support localConnectionGroup in HDDRpcClientServer");
    }

    @Override
    public RpcConfiguration rpcConfig() {
        return this.hddRpcConfig;
    }
}
