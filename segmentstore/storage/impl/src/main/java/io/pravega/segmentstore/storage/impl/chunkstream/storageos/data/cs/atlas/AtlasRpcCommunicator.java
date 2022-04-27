package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas;

import io.grpc.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas.common.Configuration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas.common.ConfigurationImpl;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.binkey.BinaryKeyBuilder;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasRuntimeException;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.AtlasProto.*;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.atlas.KeyValueStoreGrpc.newBlockingStub;

public class AtlasRpcCommunicator {

    public static final String DEFAULT_NAMESPACE = "default";   // default namespace for all keys
    public static final String SECURE_NAMESPACE = "secure";     // namespace for "secure" keys
    public static final String SYSTEM_NAMESPACE = "system";     // namespace for system keys
    private static final Logger log = LoggerFactory.getLogger(AtlasRpcCommunicator.class);
    private static final int PATH_FIELD_ID = 1;
    private static final int SUBKEY_FIELD_ID = 10;
    private static ThreadLocal<Random> threadLocalRandom = ThreadLocal.withInitial(() -> new Random());
    private final Map<String, KeyValueStoreBlockingStub> blockingStubs;
    private final Map<String, ConnStat> stats;
    private final int port;
    private final int keepAliveTimeSecs;
    private final int keepAliveTimeoutSecs;
    @Value("#{systemEnvironment['ATLAS_SERVICE_HOSTNAME'] ?: null}")
    protected String atlasServiceHostname;
    //    @Value("#{objectProperties['object.AtlasMembersFile'] ?: '/data/atlas-client/configuration/AtlasMembers'}")
    protected String atlasMembersFile;
    //    @Value("#{objectProperties['object.AtlasClientTimeout'] ?: 20000}")
    protected long requestTimeoutMs = 20000;
    protected List<String> atlasServerList;

    public AtlasRpcCommunicator(int port) {
        this(port, 0, 0);
    }

    public AtlasRpcCommunicator(int port, int keepAliveTimeSecs, int keepAliveTimeoutSecs) {
        this.port = port;
        this.keepAliveTimeSecs = keepAliveTimeSecs;
        this.keepAliveTimeoutSecs = keepAliveTimeoutSecs;
//        this.blockingStubs = new ConcurrentHashMap<>(5);
        this.blockingStubs = new ConcurrentHashMap<>(5);
        this.stats = new ConcurrentHashMap<>(5);

    }

    @PostConstruct
    public void startConfigWatch() throws CSException {
        if (atlasServiceHostname != null) {
            // kubernetes env variable ATLAS_SERVICE_HOSTNAME was set
            return;
        }

        refreshAtlasMembers();

//        try {
//            // populate initial value
//            refreshAtlasMembers();
//
//            configWatch.watch(atlasMembersFile, new ConfigWatch.Callback() {
//                @Override
//                public void changed() {
//                    refreshAtlasMembers();
//                }
//            });
//        } catch (IOException e) {
//            throw new CSException("throw IOException", e);
//        }
    }

//    private static final String DELETE_MARKER = "vnest_delete_marker";
//    private boolean hasDeleteMarker(final Configuration configuration) {
//        boolean hasMarker = configuration.getConfig(DELETE_MARKER) != null;
//        if (hasMarker) {
//            log.info("kind {} id {} has delete marker", configuration.getKind(), configuration.getId());
//        }
//        return hasMarker;
//    }

    public List<Configuration> queryAllConfiguration(String path) {


        final Set<Configuration> configSet = new TreeSet<>((o1, o2) -> o1.getId().compareTo(o2.getId()));

        try {
            ByteString token = null;//IndexKey

            do {
                var response = listKV(path, token);

                for (ListResultItem entry : response.getItemList()) {
                    ByteString value = entry.getValue();
                    Configuration c = ConfigurationImpl.parse(value.newInput());
                    configSet.add(c);
                }
                token = null;
                if (!response.getToken().isEmpty()) {
                    token = response.getToken();
                }
            } while (token != null);

        } catch (Throwable e) {
            log.error("except", e);
        }

        return new LinkedList<>(configSet);


    }

    //    @Suspendable
    private ListKeyResponse listKV(String path, ByteString token) throws CSException {
        final String ip = selectAvailableServer();
        Deadline deadline = Deadline.after(requestTimeoutMs, TimeUnit.MILLISECONDS);
        String namespace = getNamespace(false);
        final int finalCount = 1024;//LIST_MAX_KEYS_LIMIT

        ByteString prefixBytes = prefixToBytes(path, null);

        return list(ip, deadline, namespace, prefixBytes, token, finalCount);

    }

    private ByteString prefixToBytes(String path, ByteString prefixData) {
        BinaryKeyBuilder prefixBuilder = new BinaryKeyBuilder(2048);

        prefixBuilder.putString(PATH_FIELD_ID, path);

        if (prefixData != null && !prefixData.isEmpty()) {
            prefixBuilder.putSubKey(SUBKEY_FIELD_ID, prefixData);
        }

        return prefixBuilder.buildPrefix();

    }

    private ListKeyResponse list(String ip, Deadline deadline, String ns, ByteString prefix, ByteString startKey, long count) {
        KeyValueStoreBlockingStub stub = this.getBlockingStub(ip).withDeadline(deadline);

        try {
            ListKeyRequest.Builder request = ListKeyRequest.newBuilder().setNs(ns);
            if (prefix != null) {
                request.setPrefix(prefix);
            }

            if (startKey != null) {
                request.setStartKey(startKey);
            }

            if (count > 0L) {
                request.setLimit(count);
            }

            return stub.listKeys(request.build());
        } catch (StatusRuntimeException var10) {
            this.checkConnection(var10, stub, ip);
            throw new AtlasRuntimeException(var10);
        }
    }

    private void checkConnection(StatusRuntimeException e, KeyValueStoreBlockingStub stub, String remoteIp) {
        // keep alive failure channel will be shutdown.  new requests then fail with UNAVAILABLE
        if (e.getStatus().getCode() == Status.UNAVAILABLE.getCode() &&
            stub.getChannel() instanceof ManagedChannel &&
            ((ManagedChannel) stub.getChannel()).isShutdown()) {
            // remove will cause next getBlockingStub to re-create
            if (blockingStubs.remove(remoteIp) != null) {
                stats.get(remoteIp).drops++;
            }
        }
    }

    private KeyValueStoreBlockingStub getBlockingStub(String remoteIp) {
        KeyValueStoreBlockingStub stub = blockingStubs.get(remoteIp);
        if (stub == null) {
            stub = blockingStubs.computeIfAbsent(remoteIp,
                                                 k -> newBlockingStub(createChannel(remoteIp)));
        }
        return stub;
    }

    private ManagedChannel createChannel(String remoteIp) {
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(remoteIp, port)
                                                                // Channels are secure by default. We disable TLS use to avoid needing certificates.
                                                                // set channel message limit to 100MB
                                                                .maxInboundMessageSize(100 * 1024 * 1024)
                                                                .usePlaintext();

        if (keepAliveTimeSecs > 0) {
            builder
                    // if no reads in N seconds, send ping
                    .keepAliveTime(keepAliveTimeSecs, TimeUnit.SECONDS)
                    // if no read since ping within N seconds, connection is bad
                    .keepAliveTimeout(keepAliveTimeoutSecs, TimeUnit.SECONDS);
        }

        ConnStat stat = stats.get(remoteIp);
        if (stat == null) {
            stat = stats.computeIfAbsent(remoteIp, s -> new ConnStat());
        }
        stat.connections++;

        return builder.build();
    }

    protected void refreshAtlasMembers() {
        try {
            log.info("atlas membership changed, refresh from path {}", atlasMembersFile);
            File f = new File(atlasMembersFile);
            if (f.exists() && !f.isDirectory()) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                    String fileContents = reader.readLine();
                    if (fileContents != null && !fileContents.isEmpty()) {
                        log.info("Server list from Atlas Member File {} {}", f, fileContents);
                        String[] servers = fileContents.split(",");
                        setAtlasServers(Arrays.asList(servers));
                    } else {
                        log.info("empty server list from Atlas members file {}", f);
                    }
                }
            }
        } catch (Exception e) {
            log.error("error refreshing Atlas members after change", e);
        }

    }

    protected synchronized void setAtlasServers(List<String> members) {
        atlasServerList = members;
    }

    private synchronized String selectAvailableServer() throws CSException {
        if (atlasServiceHostname != null) {
            return atlasServiceHostname;
        }
        if (atlasServerList == null || atlasServerList.size() == 0) {
            log.warn("Atlas membership list is empty, will retry with exponential backoff");
            throw new CSException("Atlas membership list is empty, will retry with exponential backoff");
        }

        // choose random server, no matter the status
        // PoC only so assume all instances are up
        return atlasServerList.get(threadLocalRandom.get().nextInt(atlasServerList.size()));
    }

    private String getNamespace(boolean secure) {
        return secure ? SECURE_NAMESPACE : DEFAULT_NAMESPACE;
    }

    private static class ConnStat {
        int connections = 0;
        int drops = 0;
    }

}
