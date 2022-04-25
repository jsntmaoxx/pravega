package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import com.google.common.hash.Hashing;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Cluster {

    public static final String AFA_FILE = "/opt/emc/caspian/fabric/agent/services/object/data/nvmf";
    public static final String AFA_FILE_IN_DOCKER = "/data/nvmf";
    public final static int ssDataPort = 9099;
    public final static int ssManagePort = 9069;
    public final static int cmPort = 9091;
    public static final int DDBPort = 9971;
    public static final int CDBPort = 9981;
    private static final Logger log = LoggerFactory.getLogger(Cluster.class);
    private static final boolean IsAFACluster = Files.exists(Path.of(AFA_FILE)) || Files.exists(Path.of(AFA_FILE_IN_DOCKER));
    private static final int RGDTNumber = IsAFACluster ? 512 : 128;
    private static final int DTNumber = 128;
    private static final int RTDTNumber = 32;
    private static final int totalDTNumber = 8192;
    private static final AtomicLong requestIdGen = new AtomicLong(0);
    public static long BinSize = 10 * 1024 * 1024 * 1024L;
    // port
    public static int csInternalPort = 9969;
    protected final Map<String, NodeInfo> nodeInfoMap = new HashMap<>();
    protected String localNodeId;
    protected String localDataIp;
    protected String vdc;
    protected String varray;
    protected String rg;

    public static boolean isAFACluster() {
        return IsAFACluster;
    }

    public static long nextRequestId() {
        return requestIdGen.incrementAndGet();
    }

    private static int hash(String key, int modDTNumber) {
        return Hashing.consistentHash(Hashing.sha256().hashUnencodedChars(key), totalDTNumber) % modDTNumber;
    }

    public static void main(String[] args) {
        String namespace = "osaifeebdaedd1386de7";
        int hash = hash(namespace, 32);
        System.out.println("namespace hash is " + hash);
    }

    private static String makeDtId(String cos, String dtName, int remainder, int level, int modDTNumber) {
        String[] parts = cos.split("\\.");
        assert parts.length >= 1;
        String[] realCosParts = parts[0].split(":");
        String realCos = realCosParts[realCosParts.length - 1];
        String rg = "";
        if (parts.length > 1) {
            String[] rgParts = parts[1].split(":");
            rg = rgParts.length > 3 ? rgParts[3] : rgParts[rgParts.length - 1];
        }
        StringBuilder builder = new StringBuilder();
        builder.append("urn:storageos:OwnershipInfo:");
        builder.append(realCos);
        builder.append("_").append(rg);
        builder.append("_").append(dtName);
        builder.append("_").append(remainder);
        builder.append("_").append(modDTNumber);
        builder.append("_").append(level);
        builder.append(":");
        return builder.toString();
    }

    private static String getIP(String address) {
        return address.split(":")[0];
    }

    public String currentNodeId() {
        return localNodeId;
    }

    public String currentDataIp() {
        return localDataIp;
    }

    public String currentVdc() {
        return vdc;
    }

    public String currentCos() {
        return varray;
    }

    public String defaultRepGroup() {
        return rg;
    }

    public CompletableFuture<Boolean> ready() {
        var readyFuture = new CompletableFuture<Boolean>();
        readyFuture.complete(true);
        return readyFuture;
    }

    public abstract String deviceIdToDataIp(String deviceId) throws CSException;

    public abstract int deviceIdToRdmaIntIp(String device) throws CSException;

    public abstract String ownerIp(DTType dtType, DTLevel dtLevel, int dtIndex) throws CSException;

    public abstract void shutdown();

    public int dtNumber(DTType dtType) {
        switch (dtType) {
            case OB:
            case LS:
                return RGDTNumber;
            case RT:
                return RTDTNumber;
            case RR:
            case BR:
            case CT:
            case SS:
            case PR:
            case MT:
            case MA:
            case MR:
            case ET:
            default:
                return DTNumber;
        }
    }

    public int dtHash(DTType dtType, String id) {
        return hash(id, dtNumber(dtType));
    }

    public abstract void init();

    public abstract String dataEndpoint();

    public abstract String deviceIdToDDBServerIP(UUID device) throws CSException;

    public abstract List<String> DDBServerIPs();

    protected void updateNodeId() {
        String idFile = "/host/data/id.json";
        if (!Files.exists(Path.of(idFile))) {
            idFile = "/opt/emc/caspian/fabric/agent/services/object/main/host/data/id.json";
        }
        try (var f = new FileInputStream(idFile)) {
            localNodeId = new JSONObject(new String(f.readAllBytes())).getString("agent_id");
            log.info("get current node id {} from {}", localNodeId, idFile);
        } catch (Exception e) {
            log.error("can not get nodeId from {}, exit with -1", idFile, e);
            System.exit(-1);
        }
    }

    static class NodeInfo {
        public final String hostName;
        public final String privateIp;
        public final String publicIp;
        public final String dataIp;
        public final String dataIp2;
        public final String rdmaIp;
        public int rdmaIntIp;

        public NodeInfo(String hostName, String privateIp, String publicIp, String dataIp, String dataIp2) {
            this.hostName = hostName;
            this.privateIp = privateIp;
            this.rdmaIp = privateIp;
            try {
                this.rdmaIntIp = InetAddress.getByName(this.rdmaIp).hashCode();
            } catch (UnknownHostException e) {
                log.error("failed to convert data endpoint {} to rdma int ip", this.rdmaIp);
                this.rdmaIntIp = 0;
            }
            this.publicIp = publicIp;
            this.dataIp = dataIp;
            this.dataIp2 = dataIp2;
        }
    }
}
