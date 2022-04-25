package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.BucketGPBFactory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.bucket.Bucket.NamingRule;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.bucket.BucketMessages;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRpcServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class ResourceClientImpl extends DTClient implements ResourceClient {

    private static final Logger log = LoggerFactory.getLogger(ResourceClientImpl.class);

    private final static String IS_CHUNK_OBJECT_ENABLED = "is_chunk_object_enabled";
    private final static String CHUNK_SIZE = "chunk_size";
    private final static String CHUNK_EC_DATA_NUM = "chunk_ec_data_num";
    private final static String CHUNK_EC_CODE_NUM = "chunk_ec_code_num";
    private final static String CHUNK_NAMING_RULE = "chunk_naming_rule";


    private CSConfiguration csConfig;
    private Map<String, Bucket> nameToBuckets;
    private volatile long lastListingTimeStamp;
    private String namespace;
    private ReentrantLock lock = new ReentrantLock();

    public ResourceClientImpl(DTRpcServer rpcServer, CSConfiguration csConfig, Cluster cluster, String namespace) {
        super(rpcServer, cluster);
        this.nameToBuckets = new ConcurrentHashMap<>();
        this.namespace = namespace;
        this.csConfig = csConfig;
    }

    @PostConstruct
    public void init() {
        cluster.ready().whenComplete((complete, throwable) -> {
            if (complete)
                refreshBucketCache();
        });
    }

    @Override
    public Bucket fromName(String name) {
        Bucket bucket = nameToBuckets.get(name);
        if (bucket == null) {
            try {
                lock.lock();
                bucket = nameToBuckets.get(name);
                if (bucket == null) {
                    if (System.currentTimeMillis() - lastListingTimeStamp > 5000L) {
                        if (refreshBucketCache())
                            bucket = nameToBuckets.get(name);
                    } else {
                        log.info("last refresh at {}, skip to refresh this time", lastListingTimeStamp);
                    }

                    if (bucket == null) {
                        log.error("fail to find bucket {}", name);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return bucket;
    }

    @Override
    public Collection<Bucket> listBuckets() {
        return nameToBuckets.values();
    }

    @Override
    protected int dtServerPort() {
        return 9888;
    }

    private boolean refreshBucketCache() {

        log.info("start to refresh bucket cache");
        lastListingTimeStamp = System.currentTimeMillis();
        var newBucketCache = new ConcurrentHashMap<String, Bucket>();
        var resultFuture = queryAllBuckets(namespace, null, newBucketCache).whenComplete((complete, throwable) -> {
            if (complete) {
                nameToBuckets = newBucketCache;
            } else {
                log.error("fail to refresh buckets");
            }
        });

        try {
            resultFuture.get(1, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            log.error("exception: ", e);
        } catch (ExecutionException e) {
            log.error("exception: ", e);
        } catch (TimeoutException e) {
            log.error("exception: ", e);
        }

        return false;
    }

    private CompletableFuture<Boolean> queryAllBuckets(String namespace, SchemaKeyRecords.SchemaKey token, Map<String, Bucket> cache) {

        int keyHash = cluster.dtHash(DTType.RT, namespace);//TODO may need use system DT numbers to calculate dt hash
        log.info("we got key hash {} for namespace {}", keyHash, namespace);

        BucketMessages.ListRequest.Builder builder = BucketMessages.ListRequest.newBuilder()
                                                                               .setNamespace(namespace)
                                                                               .setMaxEntries(-1); //list all buckets
        if (token != null)
            builder.setSchemaToken(token);

        BucketMessages.ListRequest listRequest = builder.build();

        final var resultFuture = new CompletableFuture<Boolean>();

        try {
            //TODO we should create payload message with system virtual array(system storage pool),
            // but in current environment there is only one virtual array, so we don't need to access zk to fetch sys varray for now

            sendRequest(DTType.RT, DTLevel.Level0, keyHash, FileOperationsPayloads.CommandType.REQUEST_BUCKET_LIST, listRequest)
                    .whenComplete((BiConsumer<FileOperationsPayloads.FileOperationsPayload, Throwable>) (response, throwable) -> {

                        if (throwable != null) {
                            log.error("fail to query buckets in namespace {}", namespace);
                            resultFuture.complete(false);
                            return;
                        }

                        if (response.getRemoteProcessStatus() != FileOperationsPayloads.RemoteProcessStatus.READY) {
                            log.error("query buckets failed, remote process status {}", response.getRemoteProcessStatus());
                            resultFuture.complete(false);
                            return;
                        }

                        try {
                            BucketMessages.ListResponse listResponse = BucketMessages.ListResponse.parseFrom(response.getPayload());
                            log.info("fetch {} buckets from RT", listResponse.getBucketInfoCount());
                            var bucketInfos = listResponse.getBucketInfoList();
                            for (var bucketInfo : bucketInfos) {
                                var name = bucketInfo.getLabel();
                                var index = bucketInfo.hasIndexNumber() ? (int) bucketInfo.getIndexNumber() : 0;
                                Bucket bucket;
                                log.info("Bucket {} has hasKeypoolMetadata? {}", name, bucketInfo.hasKeypoolMetadata());
                                if (bucketInfo.hasKeypoolMetadata()) {
                                    var metadataMap = BucketGPBFactory.toStringMap(bucketInfo.getKeypoolMetadata());
                                    log.info("metadata is {}", metadataMap);
                                    var isChunkObjectEnabledStr = metadataMap.get(IS_CHUNK_OBJECT_ENABLED);
                                    var isChunkObjectEnabled = "true".equalsIgnoreCase(isChunkObjectEnabledStr);
                                    if (isChunkObjectEnabled) {
                                        var chunkSize = metadataMap.getOrDefault(CHUNK_SIZE, null);
                                        var ecDataNumber = Integer.parseInt(metadataMap.getOrDefault(CHUNK_EC_DATA_NUM, "-1"));
                                        var ecCodeNumber = Integer.parseInt(metadataMap.getOrDefault(CHUNK_EC_CODE_NUM, "-1"));
                                        var namingRule = NamingRule.valueOf(metadataMap.getOrDefault(CHUNK_NAMING_RULE, "HEX_KEY_RULE"));
                                        bucket = new Bucket(name, index, chunkSize, ecDataNumber, ecCodeNumber, namingRule, csConfig);
                                    } else {
                                        bucket = new Bucket(name, index, csConfig);
                                    }
                                } else {
                                    bucket = new Bucket(name, index, csConfig);
                                }
                                cache.put(name, bucket);
                                log.info("add bucket {} to cache", bucket);

                            }
                            if (listResponse.hasSchemaToken()) {
                                log.info("schema token is not null, initiate next query");
                                queryAllBuckets(namespace, listResponse.getSchemaToken(), cache).whenComplete((complete, throwable1) -> {
                                    if (throwable1 != null) {
                                        resultFuture.completeExceptionally(throwable1);
                                    } else {
                                        resultFuture.complete(complete);
                                    }
                                });
                            } else {
                                log.info("query complete");
                                resultFuture.complete(true);
                            }

                        } catch (InvalidProtocolBufferException e) {
                            log.error("fail to parse", e);
                            resultFuture.complete(false);
                        }
                    });

        } catch (CSException e) {
            log.error("Fail to refresh buckets", e);
            resultFuture.complete(false);
        } catch (IOException e) {
            log.error("Fail to refresh buckets", e);
            resultFuture.complete(false);
        }

        return resultFuture;
    }


}
