package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas.AtlasRpcCommunicator;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas.common.Configuration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTMessageBuilder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.rg.RGMessages;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.rm.OwnershipInfoRecords.OwnershipInfoRecord;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.CommandType.RESPONSE_FAILURE;

public class K8sCluster extends Cluster {
    public static final String PATH_SEP = "/";
    private static final Logger log = LoggerFactory.getLogger(K8sCluster.class);
    private static final String ZK_OWNERSHIP_TABLE_TYPE_NAME = "ZkOwnershipTable";
    private final static String LIST_CONFIG_TABLE_PATH = "/config/DS/";
    private final static String VALUE_KEY = "Value";
    private static final int LIST_MAX_ENTRIES = 100;
    private final String dataEndpoint = "127.0.0.1";
    private final List<String> nodeIds = new ArrayList<>(1);
    private final String OWNER_CONFIG_TABLE_PATH = "/config/ZkConfigTable/DS_";//ZKKVSTORE + TABLE_PREFIX_SEPARATOR + tableId.toString();
    private final ScheduledExecutorService refreshOwnerExecutor = new ScheduledThreadPoolExecutor(1);
    private int rdmaIntIp;
    private List<URI> configTableIds = new ArrayList<>();
    private volatile Map<DTType, OwnershipInfoRecord[][]> ownerMaps;
    private CompletableFuture<Boolean> ownerMapReadyFuture = new CompletableFuture<>();
    private CompletableFuture<Boolean> readyFuture = new CompletableFuture<>();
    @Autowired
    private AtlasRpcCommunicator atlasRpcCommunicator;
    @Value("#{systemEnvironment['MY_OBJECTSTORE_NAME']}")
    private String defaultRgName;
    @Value("#{systemEnvironment['MY_POD_IP']}")
    private String podIp;
    private DTRpcServer rpcServer;
    private Map<String, String> deviceId2Ip = new ConcurrentHashMap<>();//TODO, need to refresh the cache when ss pod is down

    public K8sCluster() {
        try {
            this.rdmaIntIp = InetAddress.getByName(dataEndpoint).hashCode();
        } catch (UnknownHostException e) {
            log.error("failed to convert data endpoint {} to rdma int ip", this.dataEndpoint);
            this.rdmaIntIp = 0;
        }
        nodeIds.add(UUID.randomUUID().toString());
    }

    public static ByteString stringToByteString(String value) {
        return ByteString.copyFrom(Base64.decodeBase64(value));//yen: use NoCopy in the future?
    }

    private static RGMessages.ListRequest constructListRequest(final SchemaKeyRecords.SchemaKey token, int maxEntries, boolean includeDeleted) {
        final RGMessages.ListRequest.Builder builder = RGMessages.ListRequest.newBuilder();
        if (token != null) {
            builder.setSchemaToken(token);
        }
        builder.setMaxEntries(maxEntries);
        builder.setIncludeDeleted(includeDeleted);
        return builder.build();
    }

    @Override
    public void init() {

        if (readyFuture.isDone())
            return;

        try {
            updateNodeId();
            updateNetWorkInfo();
            refreshOwnerExecutor.scheduleAtFixedRate(this::freshOwnerMap, 0, 60, TimeUnit.SECONDS);
            ownerMapReadyFuture.whenComplete((ready, throwable) -> {
                if (ready) {
                    var updateVdcInfoFuture = updateVdcInfo();
                    updateVdcInfoFuture.whenComplete((complete, throwable1) -> {
                        readyFuture.complete(true);
                    });
                }
            });

        } catch (Throwable t) {
            log.error("failed to get K8s cluster info ", t);
            System.exit(-1);
        }

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
                }
            }
            log.info("get current node id {} data endpoint {} from {}", localNodeId, localDataIp, networkFile);
        }
    }

    private void freshOwnerMap() {

        configTableIds.clear();
        List<Configuration> configs = atlasRpcCommunicator.queryAllConfiguration(LIST_CONFIG_TABLE_PATH);
        for (Configuration config : configs) {
            if (config.getId().contains(ZK_OWNERSHIP_TABLE_TYPE_NAME)) {
                configTableIds.add(URI.create(config.getId()));
            }
        }
        log.info("Fetch config tables from atlas: {}", configTableIds);

        List<ByteString> configValues = new ArrayList<>();

        for (URI tableId : configTableIds) {

            StringBuilder builder = new StringBuilder();
            builder.append(OWNER_CONFIG_TABLE_PATH).append(tableId.toString()).append(PATH_SEP);
            String path = builder.toString();
            log.info("try to get configs from zk, {}", path);
            List<Configuration> ownerConfigs = atlasRpcCommunicator.queryAllConfiguration(path);
            log.info("get {} configs from zk", ownerConfigs.size());

            for (Configuration config : ownerConfigs) {

                ByteString value = stringToByteString(config.getConfig(VALUE_KEY));
                configValues.add(value);

            }
        }

        try {
            var newOwnerMap = new HashMap<DTType, OwnershipInfoRecord[][]>();
            for (ByteString value : configValues) {
                OwnershipInfoRecord ownershipRecord = OwnershipInfoRecord.parseFrom(value);
                DTType dtType = DTType.valueOf(ownershipRecord.getType());
                int dtIndex = ownershipRecord.getRemainder();
                int dtNumber = ownershipRecord.getDivisor();
                var levelDTList = newOwnerMap.computeIfAbsent(dtType, (key) -> new OwnershipInfoRecord[DTLevel.LevelCount][dtNumber]);
                levelDTList[ownershipRecord.getLevel()][dtIndex] = ownershipRecord;
            }

            log.info("refresh owner map info completed");
            ownerMaps = newOwnerMap;
            ownerMapReadyFuture.complete(true);
        } catch (InvalidProtocolBufferException e) {
            log.error("fail to parse", e);
        }
    }

    @Override
    public String deviceIdToDataIp(String deviceId) throws CSException {

        String dataIp = deviceId2Ip.computeIfAbsent(deviceId, id -> {
            try {
                return InetAddress.getByName(id).getHostAddress();
            } catch (UnknownHostException e) {
                log.error("can not resolve deviceId {} to ip address", id);
                return null;
            }
        });

        if (dataIp == null) {
            throw new CSException("can not resolve deviceId" + deviceId + " to ip address");
        }
        return dataIp;
    }

    @Override
    public int deviceIdToRdmaIntIp(String device) {
        return rdmaIntIp;
    }

    @Override
    public String ownerIp(DTType dtType, DTLevel dtLevel, int dtIndex) throws CSException {

        if (ownerMaps != null) {
            var dtLists = ownerMaps.get(dtType);
            if (dtLists != null) {
                var record = dtLists[dtLevel.intLevel()][dtIndex];
                if (record != null) {
                    return record.getOwnerIpAddress().split(":")[0];
                }
            }

        }

        log.error("can not find owner ip by dt {} level {} index {}", dtType, dtLevel, dtIndex);
        throw new CSException("can not find owner ip");
    }

    @Override
    public void shutdown() {
    }

    @Override
    public String dataEndpoint() {
        return dataEndpoint;
    }

    @Override
    public String deviceIdToDDBServerIP(UUID device) throws CSException {
        return deviceIdToDataIp(device.toString());
    }

    @Override
    public List<String> DDBServerIPs() {
        return Collections.singletonList(dataEndpoint);
    }

    private CompletableFuture<Boolean> updateVdcInfo() {
        var resourceDirs = ownerMaps.get(DTType.RT)[0];
        var resourceDirIt = Arrays.asList(resourceDirs).iterator();

        CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();
        var future = queryRgInfo(resourceDirIt.next(), null, resourceDirIt, null);

        future.whenComplete((listResponse, throwable) -> {
            if (throwable != null) {
                log.error("Fail to update vdc info");
                resultFuture.complete(false);
                return;
            }
            if (listResponse == null) {
                log.error("null RG message");
                resultFuture.complete(false);
                return;
            }
            var rgInfoList = listResponse.getRgInfoList();
            var rgInfo = rgInfoList.get(0);
            rg = rgInfo.getId();
            vdc = rgInfo.getZoneCos(0).getZoneId();
            varray = rgInfo.getZoneCos(0).getVArray();
            log.info("updateVdcInfo complete: rg={}, vdc={}, varray={}", rg, vdc, varray);

            resultFuture.complete(true);

        });
        return resultFuture;
    }

    private CompletableFuture<RGMessages.ListResponse> queryRgInfo(OwnershipInfoRecord resourceDir,
                                                                   SchemaKeyRecords.SchemaKey token,
                                                                   Iterator<OwnershipInfoRecord> iterator,
                                                                   CompletableFuture<RGMessages.ListResponse> preResultFuture
                                                                  ) {
        final CompletableFuture<RGMessages.ListResponse> resultFuture = (preResultFuture != null) ? preResultFuture : new CompletableFuture<>();

        if (resourceDir == null) {
            log.error("ownership record is null");
            if (iterator.hasNext()) {
                return queryRgInfo(iterator.next(), null, iterator, resultFuture);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }

        var address = resourceDir.getOwnerIpAddress();
        var ipAndPort = address.split(":");
        var ip = ipAndPort[0];
        var port = Integer.valueOf(ipAndPort[1]);

        RGMessages.ListRequest request = constructListRequest(token, LIST_MAX_ENTRIES, true);
        var requestId = "r-rgm-" + nextRequestId();
        var commandType = FileOperationsPayloads.CommandType.REQUEST_RG_LIST;

        var message = DTMessageBuilder.makeFileOperationPayloads(requestId,
                                                                 commandType,
                                                                 DTType.RT,
                                                                 DTLevel.Level0,
                                                                 resourceDir.getRemainder(),
                                                                 resourceDir.getVirtualPool(),
                                                                 currentDataIp(),
                                                                 request);

        var createNano = System.nanoTime();

        CompletableFuture<? extends FileOperationsPayload> payloadFuture = null;
        try {
            payloadFuture = rpcServer.sendRequest(ip,
                                                  port,
                                                  requestId,
                                                  createNano,
                                                  DTMessageBuilder.makeByteBuf(message, createNano));


            //TODO: we must use Async here because it will call queryRgInfo->rpcServer.sendRequest->ChannelFuture.sync,
            // sync method could not be called from ChannelHandler.
            // We also need to consider whether sync method will block all threads in default executor.
            payloadFuture.whenCompleteAsync((BiConsumer<FileOperationsPayload, Throwable>) (response, throwable) -> {
                if (throwable != null) {
                    log.error("get throwable: ", throwable);
                    resultFuture.completeExceptionally(throwable);
                }

                if (response.getCommandType() == RESPONSE_FAILURE) {
                    log.error("response failure");
                    resultFuture.completeExceptionally(new Exception("response failure"));

                } else {

                    RGMessages.ListResponse listResponse = null;
                    try {
                        listResponse = RGMessages.ListResponse.parseFrom(response.getPayload());
                        //SchemaKeyRecords.SchemaKey token = listResponse.getSchemaToken();
                        var rgInfoList = listResponse.getRgInfoList();

                        rgInfoList = rgInfoList.stream().filter(rgInfo -> rgInfo.getName().equals(defaultRgName)).collect(Collectors.toList());

                        if (rgInfoList != null && rgInfoList.size() > 0) {
                            resultFuture.complete(listResponse);
                        } else {
                            queryRgInfo(iterator.next(), null, iterator, resultFuture);
                        }

                    } catch (InvalidProtocolBufferException e) {
                        log.error("fail to parse RGMessage");
                        resultFuture.completeExceptionally(new Exception("fail to parse RGMessage"));
                    }
                }
            });

        } catch (CSException e) {
            log.error("excep", e);
        } catch (IOException e) {
            log.error("excep", e);
        }

        return resultFuture;
    }

    public void setRpcServer(DTRpcServer rpcServer) {
        this.rpcServer = rpcServer;
    }

    @Override
    public CompletableFuture<Boolean> ready() {
        return readyFuture;
    }
}
