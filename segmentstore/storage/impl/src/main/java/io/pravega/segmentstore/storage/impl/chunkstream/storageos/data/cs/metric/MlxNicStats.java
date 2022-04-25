package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

public class MlxNicStats extends NicStats {
    static private final String TX_BYTES = "     tx_bytes_phy:";
    static private final String RX_BYTES = "     rx_bytes_phy:";

    public MlxNicStats(String deviceName) {
        super(deviceName);
    }

    @Override
    protected String rxPrefix() {
        return RX_BYTES;
    }

    @Override
    protected String txPrefix() {
        return TX_BYTES;
    }
}
