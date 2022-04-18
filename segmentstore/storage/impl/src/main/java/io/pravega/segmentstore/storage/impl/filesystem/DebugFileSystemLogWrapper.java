package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wrapper for a FileSystemLog which only exposes methods that should be used for debugging/admin tools.
 * NOTE: this class is not meant to be used for regular, production code. It exposes operations that should only be executed
 * from the admin tools.
 */
public class DebugFileSystemLogWrapper implements DebugDurableDataLogWrapper {

    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final FileSystemLog log;
    private final FileSystemConfig config;
    private final AtomicBoolean initialized;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DebugLogWrapper class.
     *
     * @param logId      The Id of the FileSystemLog to wrap.
     * @param zkClient   A pointer to the CuratorFramework client to use.
     * @param config     FileSystemConfig to use.
     * @param executor   An Executor to use for async operations.
     */
    DebugFileSystemLogWrapper(int logId, CuratorFramework zkClient, FileSystemConfig config, ScheduledExecutorService executor) {
        this.log = new FileSystemLog(logId, zkClient, config, executor);
        this.config = config;
        this.initialized = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.log.close();
    }

    //endregion

    //region Operations

    @Override
    public DurableDataLog asReadOnly() throws DataLogInitializationException {
        return new ReadOnlyFileSystemLog(this.log.getLogId(), this.log.loadMetadata());
    }

    /**
     * Loads a fresh copy FileSystemLog Metadata from ZooKeeper, without doing any sort of fencing or otherwise modifying
     * it.
     *
     * @return A new instance of the LogMetadata class, or null if no such metadata exists (most likely due to this being
     * the first time accessing this log).
     * @throws DataLogInitializationException If an Exception occurred.
     */
    @Override
    public ReadOnlyFileSystemLogMetadata fetchMetadata() throws DataLogInitializationException {
        return this.log.loadMetadata();
    }

    /**
     * Opens a file for reading purposes.
     *
     * @param fileMetadata FileMetadata for the file to open.
     * @return A RandomAccessFile representing the file.
     * @throws DurableDataLogException If an exception occurred.
     */
    public RandomAccessFile openFile(FileMetadata fileMetadata) throws DurableDataLogException {
        return FileLogs.openRead(fileMetadata.getFile());
    }

    /**
     * Updates the Metadata for this FileSystemLog in ZooKeeper by setting its Enabled flag to true.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void enable() throws DurableDataLogException {
        this.log.enable();
    }

    /**
     * Open the FileSystemLog (initializes it), then updates the Metadata for it in ZooKeeper by setting its
     * Enabled flag to false.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void disable() throws DurableDataLogException {
        initialize();
        this.log.disable();
    }

    /**
     * Performs a {@link FileSystemLog}-{@link Path} reconciliation for this {@link FileSystemLog} subject to the
     * following rules:
     * - Any {@link Path}s that list this {@link FileSystemLog} as their owner will be added to this {@link FileSystemLog}'s
     * list of files (if they're non-empty and haven't been truncated out).
     * - Any {@link FileMetadata} instances in this {@link FileSystemLog} that point to inexistent {@link Path}s
     * will be removed.
     *
     * @param candidateFiles A List of {@link Path}s that contain all the Files that this {@link FileSystemLog}
     *                         should contain. This could be the list of all FileSystem Files or a subset, as long as
     *                         it contains all Files that list this {@link FileSystemLog} as their owner.
     * @return True if something changed (and the metadata is updated), false otherwise.
     * @throws IllegalStateException   If this FileSystemLog is not disabled.
     * @throws DurableDataLogException If an exception occurred while updating the metadata.
     */
    public boolean reconcileFiles(List<? extends Path> candidateFiles) throws DurableDataLogException {
        // Load metadata and verify if disabled (metadata may be null if it doesn't exist).
        LogMetadata metadata = this.log.loadMetadata();
        final int highestFileCreatedSequence;
        if (metadata != null) {
            Preconditions.checkState(!metadata.isEnabled(), "FileSystemLog is enabled; cannot reconcile files.");
            int fileCount = metadata.getFiles().size();
            if (fileCount > 0) {
                // Get the highest File Created Sequence from the list of files.
                highestFileCreatedSequence = FileLogs.getFileCreatedSequence(metadata.getFiles().get(fileCount - 1).getFile());
            } else if (metadata.getTruncationAddress() != null) {
                // All Files have been truncated out. Get it from the Truncation Address.
                highestFileCreatedSequence = FileLogs.getFileCreatedSequence(metadata.getTruncationAddress().getFile());
            } else {
                // No information.
                highestFileCreatedSequence = FileLogs.NO_FILE_SEQUENCE;
            }
        } else {
            // No metadata.
            highestFileCreatedSequence = FileLogs.NO_FILE_SEQUENCE;
        }

        // First, we filter out any File that does not reference this Log as their owner or that are empty.
        candidateFiles = candidateFiles
                .stream()
                .filter(fp -> FileLogs.getFileSystemLogId(fp) == this.log.getLogId()
                        && fp.toFile().length() > 0)
                .collect(Collectors.toList());

        // Begin reconstructing the File List by eliminating references to inexistent files.
        val newFileList = new ArrayList<FileMetadata>();
        if (metadata != null) {
            List<? extends Path> finalCandidateFiles = candidateFiles;
            metadata.getFiles().stream()
                    .filter(fm -> finalCandidateFiles.contains(fm.getFile()))
                    .forEach(newFileList::add);
        }

        // Find files that should be in the log but are not referenced. Only select files which have their sequence greater
        // than the sequence of the last file used in this Log (sequence is assigned monotonically increasing, and we don't want
        // to add already truncated out files).
        val seq = new AtomicInteger(newFileList.isEmpty() ? 0 : newFileList.get(newFileList.size() - 1).getSequence());
        candidateFiles
                .stream()
                .filter(fp -> FileLogs.getFileCreatedSequence(fp) > highestFileCreatedSequence)
                .forEach(fp -> newFileList.add(new FileMetadata(fp, seq.incrementAndGet())));

        // Make sure the files are properly sorted.
        newFileList.sort(Comparator.comparingInt(FileMetadata::getSequence));

        // Determine if anything changed.
        boolean changed = metadata == null || metadata.getFiles().size() != newFileList.size();
        if (!changed) {
            for (int i = 0; i < newFileList.size(); i++) {
                if (metadata.getFiles().get(i).getFile() != newFileList.get(i).getFile()) {
                    changed = true;
                    break;
                }
            }
        }

        // Update metadata in ZooKeeper, but only if it has changed.
        if (changed) {
            val newMetadata = LogMetadata
                    .builder()
                    .enabled(false)
                    .epoch(getOrDefault(metadata, LogMetadata::getEpoch, LogMetadata.INITIAL_EPOCH) + 1)
                    .truncationAddress(getOrDefault(metadata, LogMetadata::getTruncationAddress, LogMetadata.INITIAL_TRUNCATION_ADDRESS))
                    .updateVersion(getOrDefault(metadata, LogMetadata::getUpdateVersion, LogMetadata.INITIAL_VERSION))
                    .files(newFileList)
                    .build();
            this.log.overWriteMetadata(newMetadata);
        }

        return changed;
    }

    /**
     * Allows to overwrite the metadata of a FileSystemLog. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged FileSystemLog).
     *
     * @param metadata New metadata to set in the original FileSystemLog metadata path.
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void forceMetadataOverWrite(ReadOnlyLogMetadata metadata) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize((LogMetadata) metadata).getCopy();
            this.log.getZkClient().setData().forPath(this.log.getLogNodePath(), serializedMetadata);
        } catch (Exception e) {
            throw new DurableDataLogException("Problem overwriting FileSystem Log metadata.", e);
        }
    }

    /**
     * Delete the metadata of the FileSystemLog in Zookeeper. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged FileSystemLog).
     *
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void deleteDurableLogMetadata() throws DurableDataLogException {
        try {
            this.log.getZkClient().delete().forPath(this.log.getLogNodePath());
        } catch (Exception e) {
            throw new DurableDataLogException("Problem deleting FileSystem Log metadata.", e);
        }
    }

    private void initialize() throws DurableDataLogException {
        if (this.initialized.compareAndSet(false, true)) {
            try {
                this.log.initialize(DEFAULT_TIMEOUT);
            } catch (Exception ex) {
                this.initialized.set(false);
                throw ex;
            }
        }
    }

    private <T> T getOrDefault(LogMetadata metadata, Function<LogMetadata, T> getter, T defaultValue) {
        return metadata == null ? defaultValue : getter.apply(metadata);
    }

    //endregion

    //region ReadOnlyFileSystemLog

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class ReadOnlyFileSystemLog implements DurableDataLog {
        private final int logId;
        private final LogMetadata logMetadata;

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader() {
            return new LogReader(this.logId, this.logMetadata, DebugFileSystemLogWrapper.this.config);
        }

        @Override
        public WriteSettings getWriteSettings() {
            return new WriteSettings(FileSystemConfig.MAX_APPEND_LENGTH,
                    Duration.ofMillis(FileSystemConfig.FS_WRITE_TIMEOUT.getDefaultValue()),
                    FileSystemConfig.FS_MAX_OUTSTANDING_BYTES.getDefaultValue());
        }

        @Override
        public long getEpoch() {
            return this.logMetadata.getEpoch();
        }

        @Override
        public QueueStats getQueueStatistics() {
            return null;
        }

        @Override
        public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}
