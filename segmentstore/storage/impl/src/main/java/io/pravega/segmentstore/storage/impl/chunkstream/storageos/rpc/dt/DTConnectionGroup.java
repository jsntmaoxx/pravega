package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.IPConnectionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DTConnectionGroup extends IPConnectionGroup<FileOperationsPayload> {
    private static final Logger log = LoggerFactory.getLogger(DTConnectionGroup.class);

    private final DTRequestContainer requestContainer;

    public DTConnectionGroup(RpcServer<FileOperationsPayload> rpcServer,
                             DTRequestContainer requestContainer,
                             String nodeIp,
                             int remotePort,
                             int connectionNumber) {
        super(rpcServer, nodeIp, nodeIp, remotePort, connectionNumber);
        this.requestContainer = requestContainer;
        initConnection();
    }

    @Override
    protected Connection<FileOperationsPayload> makeConnection(RpcServer<FileOperationsPayload> rpcServer) {
        return new DTConnection(rpcServer, this.remoteIp, this.remotePort, requestContainer);
    }

    @Override
    protected Logger log() {
        return log;
    }
}
