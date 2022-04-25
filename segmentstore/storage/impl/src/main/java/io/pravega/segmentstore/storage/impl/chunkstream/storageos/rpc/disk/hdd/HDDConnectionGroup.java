package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.IPConnectionGroup;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDDConnectionGroup extends IPConnectionGroup<HDDMessage> {
    private static final Logger log = LoggerFactory.getLogger(HDDConnectionGroup.class);

    public HDDConnectionGroup(RpcServer<HDDMessage> rpcServer, String nodeIp, int port, int connectionNumber) {
        super(rpcServer, nodeIp, nodeIp, port, connectionNumber);
        initConnection();
    }

    @Override
    protected Connection<HDDMessage> makeConnection(RpcServer<HDDMessage> rpcServer) {
        return new HDDConnection(rpcServer, this.remoteIp, this.remotePort);
    }

    @Override
    protected Logger log() {
        return log;
    }
}
