package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for FileSystem.
 */
public class FileSystemConfig {
    //region Config Names

    public static final Property<String> ZK_METADATA_PATH = Property.named("zk.metadata.path", "/segmentstore/containers/fs", "zkMetadataPath");
    public static final Property<Integer> ZK_HIERARCHY_DEPTH = Property.named("zk.metadata.hierarchy.depth", 2, "zkHierarchyDepth");
    public static final Property<Integer> FS_MAX_WRITE_ATTEMPTS = Property.named("fs.write.attempts.count.max", 5, "fsMaxWriteAttempts");
    public static final Property<Integer> FS_WRITE_TIMEOUT = Property.named("fs.write.timeout.milliseconds", 60000, "fsWriteTimeoutMillis");
    public static final Property<Integer> FS_READ_BATCH_SIZE = Property.named("fs.read.batch.size", 64, "fsReadBatchSize");
    public static final Property<Integer> FS_MAX_OUTSTANDING_BYTES = Property.named("fs.write.outstanding.bytes.max", 256 * 1024 * 1024, "fsMaxOutstandingBytes");
    public static final Property<Integer> FS_FILE_MAX_SIZE = Property.named("fs.file.size.max", 1024 * 1024 * 1024, "fsFileMaxSize");

    public static final String COMPONENT_CODE = "filesystem";
    /**
     * Maximum append length of FileSystemLOg.
     */
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;

    //endregion

    //region Members

    /**
     * Sub-namespace to use for ZooKeeper LogMetadata.
     */
    @Getter
    private final String zkMetadataPath;

    /**
     * Depth of the node hierarchy in ZooKeeper. 0 means flat, N means N deep, where each level is indexed by its
     * respective log id digit.
     */
    @Getter
    private final int zkHierarchyDepth;

    /**
     * The maximum number of times to attempt a write.
     */
    @Getter
    private final int fsMaxWriteAttempts;

    /**
     * The Write Timeout (File System), in milliseconds.
     */
    @Getter
    private final int fsWriteTimeoutMillis;

    /**
     * The number of File Entries to read at once from FileSystem.
     */
    @Getter
    private final int fsReadBatchSize;

    /**
     * The maximum number of bytes that can be outstanding per FileSystemLog at any given time. This value should be used
     * for throttling purposes.
     */
    @Getter
    private final int fsMaxOutstandingBytes;

    /**
     * The Maximum size of a file, in bytes. On or around this value the current file is closed and a new one
     * is created. By design, this property cannot be larger than Int.MAX_VALUE, since we want File Entry Ids to be
     * representable with an Int.
     */
    @Getter
    private final int fsFileMaxSize;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileSystemConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private FileSystemConfig(TypedProperties properties) throws ConfigurationException {
        this.zkMetadataPath = properties.get(ZK_METADATA_PATH);
        this.zkHierarchyDepth = properties.getInt(ZK_HIERARCHY_DEPTH);
        if (this.zkHierarchyDepth < 0) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a non-negative integer.",
                    ZK_HIERARCHY_DEPTH, this.zkHierarchyDepth));
        }
        this.fsMaxWriteAttempts = properties.getInt(FS_MAX_WRITE_ATTEMPTS);
        this.fsFileMaxSize = properties.getInt(FS_FILE_MAX_SIZE);
        this.fsWriteTimeoutMillis = properties.getInt(FS_WRITE_TIMEOUT);
        this.fsReadBatchSize = properties.getInt(FS_READ_BATCH_SIZE);
        if (this.fsReadBatchSize < 1) {
            throw new InvalidPropertyValueException(String.format("Property %s (%d) must be a positive integer.",
                    FS_READ_BATCH_SIZE, this.fsReadBatchSize));
        }
        this.fsMaxOutstandingBytes = properties.getInt(FS_MAX_OUTSTANDING_BYTES);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<FileSystemConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, FileSystemConfig::new);
    }

    //endregion
}
