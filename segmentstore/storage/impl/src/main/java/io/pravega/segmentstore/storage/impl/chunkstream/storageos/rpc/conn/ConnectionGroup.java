package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ConnectionGroup<ResponseMessage> {
    private final RpcServer<ResponseMessage> rpcServer;
    private final Connection<ResponseMessage>[] connections;
    private final int capacity;
    private final AtomicInteger iterate = new AtomicInteger(0);

    public ConnectionGroup(RpcServer<ResponseMessage> rpcServer, int connectionNumber) {
        this.rpcServer = rpcServer;
        this.capacity = connectionNumber;
        this.connections = new Connection[this.capacity];
    }

    protected void initConnection() {
        for (var i = 0; i < this.connections.length; ++i) {
            this.connections[i] = makeConnection(rpcServer);
        }
    }

    public Connection<ResponseMessage> lockOneConnection() {
        int count = capacity;
        boolean hasConnError = false;
        while (!hasConnError) {
            while (count-- > 0) {
                var pos = iterate.getAndIncrement() & (capacity - 1);
                var conn = connections[pos];
                if (conn.lock()) {
                    if (conn.isOpen()) {
                        return conn;
                    }
                    // cj_todo not thread save on connections.
                    // should use atomic object array
                    try {
                        if (!hasConnError) {
                            log().warn("connection {} is not open, recreate it", conn.name());
                            var cnn = makeConnection(rpcServer);
                            if (cnn.connect()) {
                                if (cnn.lock()) {
                                    connections[pos] = cnn;
                                    return cnn;
                                }
                            } else {
                                hasConnError = true;
                            }
                        }
                    } finally {
                        conn.unlock();
                    }
                }
            }
        }
        // cj_todo extend / shrink pool
        log().warn("No free connection to {}, current connection number {}", remoteName(), capacity);
        return null;
    }

    protected abstract Logger log();

    protected abstract String remoteName();

    protected abstract Connection<ResponseMessage> makeConnection(RpcServer<ResponseMessage> rpcServer);

    public void connect() {
        for (var c : connections) {
            if (!c.connect()) {
                log().error("connecting {} failed", c.name());
            }
        }
    }

    public void shutdown() {
        for (var c : connections) {
            c.shutdown();
        }
    }
}
