package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.DefaultExceptionHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.NamedForkJoinWorkerThreadFactory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcNettyServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.ConnectionGroup;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.handler.DTClientMessageHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.handler.DTServerMessageHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public class DTRpcServer extends RpcNettyServer<FileOperationsPayload> implements DTRequestContainer<FileOperationsPayload> {
    private static final Logger log = LoggerFactory.getLogger(DTRpcServer.class);
    private final DTRpcConfiguration dtRpcConfig;
    private final EventLoopGroup workerGroup;
    private final ExecutorService executor;
    private final int port = Cluster.csInternalPort;
    private final Bootstrap bootstrap;
    private final Map<String, CompletableFuture<FileOperationsPayload>> responseMap = new ConcurrentHashMap<>(1024);
    private ChannelFuture bindChannelFuture;

    public DTRpcServer(DTRpcConfiguration rpcConfig) {
        this.dtRpcConfig = rpcConfig;
        this.workerGroup = new NioEventLoopGroup(rpcConfig.nettyServiceEventLoopNumber());
        this.executor = new ForkJoinPool(rpcConfig.nettyServiceExecutorWorkerNumber(),
                                         new NamedForkJoinWorkerThreadFactory("dt-sever-w-"),
                                         new DefaultExceptionHandler(log),
                                         true);
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
    }

    @Override
    public void start() throws InterruptedException {
        var rpcServer = this;
        log.info("Start dt server on port {}", port);
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(workerGroup, workerGroup)
                 .channel(NioServerSocketChannel.class)
                 .childHandler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     public void initChannel(SocketChannel ch) {
                         ch.pipeline().addLast(new DTServerMessageHandler(executor, rpcServer));
                     }
                 })
                 .option(ChannelOption.SO_BACKLOG, 1024)
                 .childOption(ChannelOption.SO_KEEPALIVE, true);
        bindChannelFuture = bootstrap.bind(port).sync();
    }

    @Override
    public void join() throws InterruptedException {
        bindChannelFuture.channel().closeFuture().sync();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        log.info("Stop dt server on port {}", port);
        var channel = bindChannelFuture.channel();
        try {
            if (channel.isOpen()) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            log.error("close bind channel {}=>{} failed",
                      channel.remoteAddress(), channel.localAddress(), e);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    /*
     * DTRequestContainer
     */
    @Override
    public ChannelFuture connect(SocketAddress socketAddress, Connection<FileOperationsPayload> connection) {
        var rpcServer = this;
        var bs = bootstrap.clone();
        bs.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new DTClientMessageHandler(executor, rpcServer, connection));
            }
        });
        return bs.connect(socketAddress);
    }

    @Override
    public Logger log() {
        return log;
    }

    @Override
    public boolean storeRequest(String requestId, CompletableFuture<FileOperationsPayload> retFuture, String connectionName) {
        if (responseMap.putIfAbsent(requestId, retFuture) != null) {
            log.error("request {} from connection {} exist", requestId, connectionName);
            retFuture.completeExceptionally(new CSException("request " + requestId + " has been exist"));
            return false;
        }
        return true;
    }

    public void complete(FileOperationsPayload response) {
        var f = responseMap.remove(response.getRequestId());
        if (f == null) {
            log.error("can not find complete future of response {} command type {}",
                      response.getRequestId(), response.getCommandType());
            return;
        }
        // cj_todo perf counter request finished
        f.complete(response);
    }

    @Override
    public void completeOnSendFailure(String requestId, Throwable cause) {
        var f = responseMap.remove(requestId);
        if (f == null) {
            log.error("can not find complete future of response {} when encounter send failure",
                      requestId, cause);
            return;
        }
        f.completeExceptionally(cause);
    }

    @Override
    protected ConnectionGroup<FileOperationsPayload> connectionGroup(String nodeIp, int port) {
        var subMap = deviceConnGroupMap.computeIfAbsent(nodeIp, (ni) -> new ConcurrentHashMap<>());
        return subMap.computeIfAbsent(port, (p) -> {
            var group = new DTConnectionGroup(this, this, nodeIp, port, dtRpcConfig.connectionNumber());
            group.connect();
            return group;
        });
    }

    @Override
    protected ConnectionGroup<FileOperationsPayload> localConnectionGroup() {
        throw new UnsupportedOperationException("not support localConnectionGroup in DTRpcServer");
    }

    @Override
    public RpcConfiguration rpcConfig() {
        return this.dtRpcConfig;
    }
}
