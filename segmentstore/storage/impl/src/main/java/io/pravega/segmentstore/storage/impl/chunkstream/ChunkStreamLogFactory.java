package io.pravega.segmentstore.storage.impl.chunkstream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.CmDummyTestClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.DummyCluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.HDDClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.HDDRpcClientServer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.HDDRpcConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg.HDDMessage;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for ChunkStreamLogs.
 */
@Slf4j
public class ChunkStreamLogFactory implements DurableDataLogFactory {
    //region Members

    // Period of inspection to meet the maximum number of log creation attempts for a given container.
    private static final Duration LOG_CREATION_INSPECTION_PERIOD = Duration.ofSeconds(60);
    // Maximum number of log creation attempts for a given container before considering resetting the BK client.
    private static final int MAX_CREATE_ATTEMPTS_PER_LOG = 2;

    private final String namespace;
    private final CuratorFramework zkClient;
    private final AtomicReference<CmClient> cmClient;
    private final ChunkConfig chunkConfig;
    private final CSConfiguration csConfig;
    private final ChunkStreamConfig config;
    private final ScheduledExecutorService executor;
    @GuardedBy("this")
    private final Map<Integer, LogInitializationRecord> logInitializationTracker = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<Timer> lastCmClientReset = new AtomicReference<>(new Timer());

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ChunkStreamLogFactory class.
     *
     * @param config   The configuration to use for all instances created.
     * @param zkClient ZooKeeper Client to use.
     * @param executor An executor to use for async operations.
     */
    public ChunkStreamLogFactory(ChunkStreamConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
        this.csConfig = new CSConfiguration("16M", 8, 4);
        this.chunkConfig = new ChunkConfig("16M", 8, 4, csConfig);
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.namespace = zkClient.getNamespace();
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient")
                .usingNamespace(this.namespace + this.config.getZkMetadataPath());
        this.cmClient = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        val cm = this.cmClient.getAndSet(null);
        if (cm != null) {
            try {
                // cm.close();
            } catch (Exception ex) {
                log.error("Unable to close cm client.", ex);
            }
        }
    }

    //endregion

    //region DurableDataLogFactory Implementation

    @Override
    public void initialize() throws DurableDataLogException {
        Preconditions.checkState(this.cmClient.get() == null, "ChunkStreamLogFactory is already initialized.");
        try {
            this.cmClient.set(startCmClient());
        } catch (IllegalArgumentException | NullPointerException ex) {
            // Most likely a configuration issue; re-throw as is.
            close();
            throw ex;
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // Make sure we close anything we may have opened.
                close();
            }

            // ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to establish connection to ZooKeeper or Chunk Stream.", ex);
        }
    }

    @Override
    public DurableDataLog createDurableDataLog(int logId) {
        Preconditions.checkState(this.cmClient.get() != null, "BookKeeperLogFactory is not initialized.");
        tryResetCmClient(logId);
        return new ChunkStreamLog(logId, this.zkClient, this.cmClient.get(), this.chunkConfig, this.config, this.executor);
    }

    @Override
    public DebugChunkStreamLogWrapper createDebugLogWrapper(int logId) {
        Preconditions.checkState(this.cmClient.get() != null, "BookKeeperLogFactory is not initialized.");
        tryResetCmClient(logId);
        return new DebugChunkStreamLogWrapper(logId, this.zkClient, this.cmClient.get(), this.chunkConfig, this.config, this.executor);
    }

    @Override
    public int getRepairLogId() {
        return ChunkStreams.REPAIR_LOG_ID;
    }

    @Override
    public int getBackupLogId() {
        return ChunkStreams.BACKUP_LOG_ID;
    }

    /**
     * Gets a pointer to the cm client used by this ChunkStreamLogFactory. This should only be used for testing or
     * admin tool purposes only. It should not be used for regular operations.
     *
     * @return The cm client.
     */
    @VisibleForTesting
    public CmClient getCmClient() {
        return this.cmClient.get();
    }

    //endregion

    //region Initialization

    private CmClient startCmClient() {
        HDDRpcConfiguration hddRpcConfig = new HDDRpcConfiguration();
        DiskRpcClientServer<HDDMessage> diskRpcClientServer = new HDDRpcClientServer(hddRpcConfig);
        Cluster cluster = new DummyCluster();
        DiskClient<? extends DiskMessage> diskClient = new HDDClient(diskRpcClientServer, this.csConfig, cluster);
        return new CmDummyTestClient(this.csConfig, diskClient);
    }

    /**
     * Recreate the cm client if a given log exhibits MAX_CREATE_ATTEMPTS_PER_LOG creation attempts (as a proxy
     * for Container recoveries) within the period of time defined in LOG_CREATION_INSPECTION_PERIOD.
     *
     * @param logId id of the log being restarted.
     */
    private void tryResetCmClient(int logId) {
        synchronized (this) {
            LogInitializationRecord record = logInitializationTracker.get(logId);
            if (record != null) {
                // Account for a restart of the Bookkeeper log.
                record.incrementLogCreations();
                // If the number of restarts for a single container is meets the threshold, let's reset the BK client.
                if (record.isCmClientResetNeeded()
                        && lastCmClientReset.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                    try {
                        log.info("Start creating cm client in reset.");
                        CmClient newClient = startCmClient();
                        // If we have been able to create a new client successfully, reset the current one and update timer.
                        log.info("Successfully created new cm client, setting it as the new one to use.");
                        CmClient oldClient = this.cmClient.getAndSet(newClient);
                        lastCmClientReset.set(new Timer());
                        // Lastly, attempt to close the old client.
                        log.info("Attempting to close old client.");
                        // oldClient.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Failure resetting the cm client", e);
                    }
                }
            } else {
                logInitializationTracker.put(logId, new LogInitializationRecord());
            }
        }
    }

    //endregion

    /**
     * Keeps track of the number of log creation attempts within an inspection period.
     */
    static class LogInitializationRecord {
        private final AtomicReference<Timer> timer = new AtomicReference<>(new Timer());
        private final AtomicInteger counter = new AtomicInteger(0);

        /**
         * Returns whether the cm client should be reset based on the max allowed attempts of re-creating a
         * log within the inspection period.
         *
         * @return whether to re-create the cm client or not.
         */
        boolean isCmClientResetNeeded() {
            return timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) < 0 && counter.get() >= MAX_CREATE_ATTEMPTS_PER_LOG;
        }

        /**
         * Increments the counter for log restarts within a particular inspection period. If the las sample is older
         * than the inspection period, the timer and the counter are reset.
         */
        void incrementLogCreations() {
            // If the time since the last log creation is too far, we need to refresh the timer to the new inspection
            // period and set the counter of log creations to 1.
            if (timer.get().getElapsed().compareTo(LOG_CREATION_INSPECTION_PERIOD) > 0) {
                timer.set(new Timer());
                counter.set(1);
            } else {
                // Otherwise, just increment the counter.
                counter.incrementAndGet();
            }
        }
    }
}
