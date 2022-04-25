package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.MlxNicStats;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.NicStats;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ECSCluster extends Cluster {
    private static final Logger log = LoggerFactory.getLogger(ECSCluster.class);
    //OwnershipInfo: [id: urn:storageos:OwnershipInfo:3a80e678-f0f8-43ba-916a-eb8004cac4f3_00000000-0000-0000-0000-000000000000_MA_99_128_0:], [owner: 10.247.99.133:9203], [cos: urn:storageos:VirtualArray:3a80e678-f0f8-43ba-916a-eb8004cac4f3.urn:storageos:ReplicationGroupInfo:00000000-0000-0000-0000-000000000000:global], [version: 6], [epoch: 1], [zkEpoch: 777c7973-7f2a-4ee9-b702-47f659535b93], [creationCompleted: true], [updateSequence: 2], [lastChangeCause: RECLAIM]
    private static final Pattern ownerRecordRegex = Pattern.compile("^\\s*OwnershipInfo:\\s*\\[id:\\s*([^]]+)],\\s*\\[owner:\\s*([^]]+)],\\s*\\[cos:\\s*([^]]+)].*");

    static {
        var nicNames = NicStats.nicNames();
        if (nicNames.contains("slave-0")) {
            Metrics.makeStatsMetric("nic-public-0", new NicStats("slave-0"));
        }
        if (nicNames.contains("slave-1")) {
            Metrics.makeStatsMetric("nic-public-1", new NicStats("slave-1"));
        }
        if (isAFACluster()) {
            if (nicNames.contains("pslave-0")) {
                Metrics.makeStatsMetric("nic-mlx-0", new MlxNicStats("pslave-0"));
            }
            if (nicNames.contains("pslave-1")) {
                Metrics.makeStatsMetric("nic-mlx-1", new MlxNicStats("pslave-1"));
            }
        } else {
            if (nicNames.contains("pslave-0")) {
                Metrics.makeStatsMetric("nic-private-0", new NicStats("pslave-0"));
            }
            if (nicNames.contains("pslave-1")) {
                Metrics.makeStatsMetric("nic-private-1", new NicStats("pslave-1"));
            }
        }
    }


    private final ScheduledExecutorService refreshOwnerExecutor = new ScheduledThreadPoolExecutor(1);
    private String dtQueryEndpoint;

    private boolean initialize = false;
    private volatile Map<DTType, DTRecord[][]> ownerMaps;

    public ECSCluster() {
    }


    private void updateNetWorkInfo() throws IOException {
        String networkFile = "/host/data/object-main_network.json";
        if (!Files.exists(Path.of(networkFile))) {
            networkFile = "/opt/emc/caspian/fabric/agent/services/object/main/host/data/object-main_network.json";
        }
        try (var f = new FileInputStream(networkFile)) {
            for (var o : new JSONObject(new String(f.readAllBytes())).getJSONArray("cluster_info")) {
                var jo = (JSONObject) o;
                var id = jo.getString("agent_id");
                var netJo = jo.getJSONObject("network");
                var info = new NodeInfo(netJo.getString("hostname"),
                                        netJo.getString("private_ip"),
                                        netJo.getString("public_ip"),
                                        netJo.getString("data_ip"),
                                        netJo.getString("data2_ip"));
                nodeInfoMap.put(id, info);
                if (id.equals(localNodeId)) {
                    localDataIp = info.dataIp;
                    dtQueryEndpoint = info.privateIp;
                }
            }
            log.info("get current node id {} data endpoint {} dt query endpoint {} from {}", localNodeId, localDataIp, dtQueryEndpoint, networkFile);
        }
    }

    private void updateVdcInfo() throws IOException, CSException {
        var cmd = "http://" + dtQueryEndpoint + ":9101/diagnostic/rgs";
        var conn = (HttpURLConnection) new URL(cmd).openConnection();
        conn.connect();
        if (conn.getResponseCode() != 200) {
            log.error("run {} failed", cmd);
            throw new CSException("query vdc info failed");
        }
        var reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("etag:")) {
                break;
            }
            if (line.startsWith("id: \"urn:storageos:ReplicationGroupInfo")) {
                rg = line.split("\"")[1];
                log.info("rg     {}", rg);
                continue;
            }
            if (line.trim().startsWith("zoneId:")) {
                vdc = line.split("\"")[1];
                log.info("vdc    {}", vdc);
                continue;
            }
            if (line.trim().startsWith("vArray:")) {
                varray = line.split("\"")[1];
                log.info("varray {}", varray);
            }
        }
    }

    private void freshOwnerMap() {
        try {
            var cmd = "http://" + dtQueryEndpoint + ":9101/diagnostic/DumpOwnershipInfo";
            var conn = (HttpURLConnection) new URL(cmd).openConnection();
            conn.connect();
            var ret = conn.getResponseCode();
            if (ret != 200) {
                log.error("run {} failed", cmd);
                throw new CSException("query vdc info failed");
            }
            var newOwnerMap = new HashMap<DTType, DTRecord[][]>();
            var reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                var m = ownerRecordRegex.matcher(line);
                if (m.matches()) {
                    var record = new DTRecord(m.group(1), m.group(2), m.group(3));
                    var levelDTList = newOwnerMap.computeIfAbsent(record.dtType, (key) -> new DTRecord[DTLevel.LevelCount][record.dtNumber]);
                    levelDTList[record.dtLevel.intLevel()][record.dtIndex] = record;
                }
            }
            log.info("refresh owner map info by cmd {}", cmd);
            ownerMaps = newOwnerMap;
        } catch (Exception e) {
            log.error("fresh owner map failed", e);
        }
    }


    @Override
    public String deviceIdToDataIp(String deviceId) throws CSException {
        var node = nodeInfoMap.get(deviceId);
        if (node != null) {
            return node.dataIp;
        }
        log.error("can not find date ip by device id {}", deviceId);
        throw new CSException("can not find date ip by device id " + deviceId);
    }

    @Override
    public int deviceIdToRdmaIntIp(String deviceId) throws CSException {
        var node = nodeInfoMap.get(deviceId);
        if (node != null) {
            return node.rdmaIntIp;
        }
        log.error("can not find rdma int ip by device id {}", deviceId);
        throw new CSException("can not find rdma int ip by device id " + deviceId);
    }

    @Override
    public String ownerIp(DTType dtType, DTLevel dtLevel, int dtIndex) throws CSException {
        if (ownerMaps != null) {
            var dtList = ownerMaps.get(dtType);
            if (dtList != null) {
                var record = dtList[dtLevel.intLevel()][dtIndex];
                if (record != null) {
                    return record.ownerIp;
                }
            }
        }
        log.error("can not find owner ip by dt {} level {} index {}", dtType, dtLevel, dtIndex);
        throw new CSException("can not find owner ip");
    }

    @Override
    public void shutdown() {
        refreshOwnerExecutor.shutdown();
    }

    @Override
    public void init() {
        if (initialize) {
            return;
        }
        try {
            updateNodeId();
            updateNetWorkInfo();
            updateVdcInfo();
            refreshOwnerExecutor.scheduleAtFixedRate(this::freshOwnerMap, 0, 60, TimeUnit.SECONDS);
            initialize = true;
        } catch (Throwable e) {
            log.error("failed to get ECS cluster info ", e);
            System.exit(-1);
        }
    }

    @Override
    public String dataEndpoint() {
        return currentDataIp();
    }

    @Override
    public String deviceIdToDDBServerIP(UUID deviceId) throws CSException {
        // cj_todo remove this uuid to string convert
        var node = nodeInfoMap.get(deviceId.toString());
        if (node != null) {
            return node.dataIp;
        }
        log.error("can not find data id by device id {}", deviceId);
        throw new CSException("can not find data ip by device id " + deviceId);
    }

    @Override
    public List<String> DDBServerIPs() {
        if (nodeInfoMap.isEmpty()) {
            return Collections.emptyList();
        }
        return nodeInfoMap.values().stream().map(i -> i.dataIp).collect(Collectors.toList());
    }


    //OwnershipInfo: [id: urn:storageos:OwnershipInfo:5b56b97e-23e6-43ff-8b48-0878ed4c4c4d__SS_42_128_2:],
    // [owner: 10.247.99.133:1095],
    // [cos: urn:storageos:VirtualArray:5b56b97e-23e6-43ff-8b48-0878ed4c4c4d],
    // [version: 6],
    // [epoch: 1],
    // [zkEpoch: 6f91a07b-1858-4dd7-9cc9-788398ddb26d],
    // [creationCompleted: true],
    // [updateSequence: 1],
    // [lastChangeCause: SET_CREATE_COMPLETE]
    private static class DTRecord {
        DTType dtType;
        DTLevel dtLevel;
        int dtIndex;
        int dtNumber;
        String dtId;
        String ownerIp;
        int ownerPort;
        String varray;

        public DTRecord(String dtId, String ownerAddress, String varray) {
            var dtParts = dtId.substring(0, dtId.length() - 1).split("_");
            this.dtType = DTType.valueOf(dtParts[2]);
            this.dtLevel = DTLevel.valueOf(Integer.parseInt(dtParts[5]));
            this.dtIndex = Integer.parseInt(dtParts[3]);
            this.dtNumber = Integer.parseInt(dtParts[4]);
            this.dtId = dtId;
            var ipParts = ownerAddress.split(":");
            this.ownerIp = ipParts[0];
            this.ownerPort = Integer.parseInt(ipParts[1]);
            this.varray = varray;
        }
    }
}
