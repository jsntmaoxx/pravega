package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for FileSystemLogs.
 */
@Slf4j
public class FileSystemLogFactory implements DurableDataLogFactory {
    //region Members

    private final String namespace;
    private final CuratorFramework zkClient;
    private final FileSystemConfig config;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileSystemLogFactory class.
     *
     * @param config   The configuration to use for all instances created.
     * @param zkClient ZooKeeper Client to use.
     * @param executor An executor to use for async operations.
     */
    public FileSystemLogFactory(FileSystemConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.namespace = zkClient.getNamespace();
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient")
                .usingNamespace(this.namespace + this.config.getZkMetadataPath());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        log.info("Close FileSystemLogFactory.");
    }

    //endregion

    //region DurableDataLogFactory Implementation

    @Override
    public void initialize() throws DurableDataLogException {
        log.info("Initialize FileSystemLogFactory.");
    }

    @Override
    public DurableDataLog createDurableDataLog(int logId) {
        return new FileSystemLog(logId, this.zkClient, this.config, this.executor);
    }

    @Override
    public DebugFileSystemLogWrapper createDebugLogWrapper(int logId) {
        return new DebugFileSystemLogWrapper(logId, this.zkClient, this.config, this.executor);
    }

    @Override
    public int getRepairLogId() {
        return FileLogs.REPAIR_LOG_ID;
    }

    @Override
    public int getBackupLogId() {
        return FileLogs.BACKUP_LOG_ID;
    }

    //endregion
}
