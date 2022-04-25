package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.RpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.Connection;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.conn.UDSConnectionGroup;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg.NVMeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVMeConnectionGroup extends UDSConnectionGroup<NVMeMessage> {
    private static final Logger log = LoggerFactory.getLogger(NVMeConnectionGroup.class);

    private final boolean enableShareMem;

    public NVMeConnectionGroup(RpcServer<NVMeMessage> rpcServer, String udsPath, int connectionNumber, boolean enableShareMem) {
        super(rpcServer, udsPath, connectionNumber);
        this.enableShareMem = enableShareMem;
        initConnection();
    }

    @Override
    protected Connection<NVMeMessage> makeConnection(RpcServer<NVMeMessage> rpcServer) {
        return enableShareMem ? new NVMeShareMemConnection(rpcServer, udsPath, rpcServer.rpcConfig()) : new NVMeConnection(rpcServer, udsPath);
    }

    @Override
    protected Logger log() {
        return log;
    }
}
