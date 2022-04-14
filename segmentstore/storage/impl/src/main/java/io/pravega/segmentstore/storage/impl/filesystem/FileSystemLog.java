package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBufUtil;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.ThrottlerSourceListenerCollection;
import io.pravega.segmentstore.storage.WriteFailureException;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.WriteTooLongException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@ThreadSafe
public class FileSystemLog implements DurableDataLog {

    //region Members

    private static final long REPORT_INTERVAL = 1000;
    @Getter
    private final int logId;
    @Getter(AccessLevel.PACKAGE)
    private final String logNodePath;
    @Getter(AccessLevel.PACKAGE)
    private final CuratorFramework zkClient;
    private final FileSystemConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object lock = new Object();
    private final String traceObjectId;
    @GuardedBy("lock")
    private WriteFile writeFile;
    @GuardedBy("lock")
    private LogMetadata logMetadata;
    private final WriteQueue writes;
    private final SequentialAsyncProcessor writeProcessor;
    private final SequentialAsyncProcessor rolloverProcessor;
    private final FileSystemMetrics.FileSystemLog metrics;
    private final ScheduledFuture<?> metricReporter;
    private final ThrottlerSourceListenerCollection queueStateChangeListeners;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileSystemLog class.
     *
     * @param containerId     The Id of the Container whose FileSystemLog to open.
     * @param zkClient        A reference to the CuratorFramework client to use.
     * @param config          Configuration to use.
     * @param executorService An Executor to use for async operations.
     */
    FileSystemLog(int containerId, CuratorFramework zkClient, FileSystemConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative integer.");
        this.logId = containerId;
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.closed = new AtomicBoolean();
        this.logNodePath = HierarchyUtils.getPath(containerId, this.config.getFsZkHierarchyDepth());
        this.traceObjectId = String.format("Log[%d]", containerId);
        this.writes = new WriteQueue();
        val retry = createRetryPolicy(this.config.getFsMaxWriteAttempts(), this.config.getFsWriteTimeoutMillis());
        this.writeProcessor = new SequentialAsyncProcessor(this::processWritesSync, retry, this::handleWriteProcessorFailures, this.executorService);
        this.rolloverProcessor = new SequentialAsyncProcessor(this::rollover, retry, this::handleRolloverFailure, this.executorService);
        this.metrics = new FileSystemMetrics.FileSystemLog(containerId);
        this.metricReporter = this.executorService.scheduleWithFixedDelay(this::reportMetrics, REPORT_INTERVAL, REPORT_INTERVAL, TimeUnit.MILLISECONDS);
        this.queueStateChangeListeners = new ThrottlerSourceListenerCollection();
    }

    private Retry.RetryAndThrowBase<? extends Exception> createRetryPolicy(int maxWriteAttempts, int writeTimeout) {
        int initialDelay = writeTimeout / maxWriteAttempts;
        int maxDelay = writeTimeout * maxWriteAttempts;
        return Retry.withExpBackoff(initialDelay, 2, maxWriteAttempts, maxDelay)
                .retryWhen(ex -> true); // Retry for every exception.
    }

    private void handleWriteProcessorFailures(Throwable exception) {
        log.warn("{}: Too many write processor failures; closing.", this.traceObjectId, exception);
        close();
    }

    private void handleRolloverFailure(Throwable exception) {
        log.warn("{}: Too many rollover failures; closing.", this.traceObjectId, exception);
        close();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.metricReporter.cancel(true);
            this.metrics.close();
            this.rolloverProcessor.close();
            this.writeProcessor.close();

            // Close active file.
            WriteFile writeFile;
            synchronized (this.lock) {
                writeFile = this.writeFile;
                this.writeFile = null;
                this.logMetadata = null;
            }

            // Close the write queue and cancel the pending writes.
            this.writes.close().forEach(w -> w.fail(new ObjectClosedException(this), true));

            if (writeFile != null) {
                try {
                    writeFile.setClosed(true);
                    FileLogs.close(writeFile.raf, writeFile.file);
                } catch (DurableDataLogException ex) {
                    log.error("{}: Unable to close file {}.", this.traceObjectId, writeFile.file, ex);
                }
            }

            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region DurableDataLog Implementation

    /**
     * Open this FileSystemLog log using the following protocol:
     * 1. Read Log Metadata from ZooKeeper.
     * 2. Open at least the last 2 files in the file list.
     * 3. Create a new file.
     * 3.1 If any of the steps so far fails, the process is interrupted at the point of failure, and no cleanup is attempted.
     * 4. Update Log Metadata using compare-and-set (this update contains the new file and new epoch).
     * 4.1 If CAS fails on metadata update, the newly created file is deleted (this means we were fenced out by some
     * other instance) and no other update is performed.
     *
     * @param timeout Timeout for the operation.
     * @throws DataLogWriterNotPrimaryException If we were fenced-out during this process.
     * @throws DataLogDisabledException         If the FileSystemLog is disabled. No fencing is attempted in this case.
     * @throws DataLogInitializationException   If a general initialization error occurred.
     * @throws DurableDataLogException          If another type of exception occurred.
     */
    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        List<Path> filesToDelete;
        LogMetadata newMetadata;
        synchronized (this.lock) {
            Preconditions.checkState(this.writeFile == null, "FileSystemLog is already initialized.");
            assert this.logMetadata == null : "writeFile == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata oldMetadata = loadMetadata();

            if (oldMetadata != null) {
                if (!oldMetadata.isEnabled()) {
                    throw new DataLogDisabledException("FileSystemLog is disabled. Cannot initialize.");
                }

                // Open files.
                val emptyFiles = FileLogs.open(oldMetadata.getFiles(), this.traceObjectId);

                // Update Metadata to reflect those newly found empty files.
                oldMetadata = oldMetadata.updateFileStatus(emptyFiles);
            }

            // Create new file.
            Path newFile = FileLogs.create(this.logId);
            log.info("{}: Created File {}.", this.traceObjectId, newFile);

            // Update Metadata with new File and persist to ZooKeeper.
            newMetadata = updateMetadata(oldMetadata, newFile, true);
            FileMetadata fileMetadata = newMetadata.getFile(newFile);
            assert fileMetadata != null : "cannot find newly added file metadata";
            RandomAccessFile newRaf = FileLogs.openWrite(newFile);
            this.writeFile = new WriteFile(newFile, newRaf, fileMetadata);
            this.logMetadata = newMetadata;
            filesToDelete = getFilesToDelete(oldMetadata, newMetadata);
        }

        // Delete the orphaned files from FileSystem.
        filesToDelete.forEach(file -> {
            try {
                FileLogs.delete(file);
                log.info("{}: Deleted orphan empty file {}.", this.traceObjectId, file);
            } catch (DurableDataLogException ex) {
                // A failure here has no effect on the initialization of FileSystemLog. In this case, the (empty) file
                // will remain in FileSystem until manually deleted by a cleanup tool.
                log.warn("{}: Unable to delete orphan empty file {}.", this.traceObjectId, file, ex);
            }
        });
        log.info("{}: Initialized (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, newMetadata.getEpoch(), newMetadata.getUpdateVersion());
    }

    @Override
    public void enable() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.lock) {
            Preconditions.checkState(this.writeFile == null, "FileSystemLog is already initialized; cannot re-enable.");
            assert this.logMetadata == null : "writeFile == null but logMetadata != null";

            // Load the current metadata. enable it, and then persist it back.
            LogMetadata metadata = loadMetadata();
            Preconditions.checkState(metadata == null || !metadata.isEnabled(), "FileSystemLog is already enabled.");
            if (metadata != null) {
                metadata = metadata.asEnabled();
                persistMetadata(metadata, false);
            }
            log.info("{}: Enabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata == null ? LogMetadata.INITIAL_EPOCH : metadata.getEpoch(), metadata == null ? LogMetadata.INITIAL_VERSION : metadata.getUpdateVersion());
        }
    }

    @Override
    public void disable() throws DurableDataLogException {
        // Get the current metadata, disable it, and then persist it back.
        synchronized (this.lock) {
            ensurePreconditions();
            LogMetadata metadata = getLogMetadata();
            Preconditions.checkState(metadata.isEnabled(), "FileSystemLog is already disabled.");
            metadata = this.logMetadata.asDisabled();
            persistMetadata(metadata, false);
            this.logMetadata = metadata;
            log.info("{}: Disabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata.getEpoch(), metadata.getUpdateVersion());
        }

        // Close this instance of the FileSystemLog. This ensures the proper cancellation of any ongoing writes.
        close();
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        ensurePreconditions();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "append", data.getLength());
        if (data.getLength() > FileSystemConfig.MAX_APPEND_LENGTH) {
            return Futures.failedFuture(new WriteTooLongException(data.getLength(), FileSystemConfig.MAX_APPEND_LENGTH));
        }

        Timer timer = new Timer();

        // Queue up the write.
        CompletableFuture<LogAddress> result = new CompletableFuture<>();
        this.writes.add(new Write(data, getWriteFile(), result));

        // Trigger Write Processor.
        this.writeProcessor.runAsync();

        // Post append tasks. We do not need to wait for these to happen before returning the call.
        result.whenCompleteAsync((address, ex) -> {
            if (ex != null) {
                handleWriteException(ex);
            } else {
                // Update metrics and take care of other logging tasks.
                this.metrics.writeCompleted(timer.getElapsed());
                LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, data.getLength(), address);
            }
        }, this.executorService);
        return result;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        Preconditions.checkArgument(upToAddress instanceof FileAddress, "upToAddress must be of type FileAddress.");
        return CompletableFuture.runAsync(() -> tryTruncate((FileAddress) upToAddress), this.executorService);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        ensurePreconditions();
        return new LogReader(this.logId, getLogMetadata(), this.config);
    }

    @Override
    public WriteSettings getWriteSettings() {
        return new WriteSettings(FileSystemConfig.MAX_APPEND_LENGTH,
                Duration.ofMillis(this.config.getFsWriteTimeoutMillis()),
                this.config.getFsMaxOutstandingBytes());
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        return getLogMetadata().getEpoch();
    }

    @Override
    public QueueStats getQueueStatistics() {
        return this.writes.getStatistics();
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        this.queueStateChangeListeners.register(listener);
    }

    //region Writes

    /**
     * Write Processor main loop. This method is not thread safe and should only be invoked as part of the Write Processor.
     */
    private void processWritesSync() {
        if (this.closed.get()) {
            // FileSystemLog is closed. No point in trying anything else.
            return;
        }

        if (getWriteFile().isClosed()) {
            // The size of the current file exceeds the limit. Execute the rollover processor to safely create a new file. This will reinvoke
            // the write processor upon finish, so the writes can be reattempted.
            this.rolloverProcessor.runAsync();
        } else if (!processPendingWrites() && !this.closed.get()) {
            // We were not able to complete execution of all writes. Try again.
            this.writeProcessor.runAsync();
        }
    }

    /**
     * Executes pending Writes to FileSystem. This method is not thread safe and should only be invoked as part of
     * the Write Processor.
     * @return True if the no errors, false if at least one write failed.
     */
    private boolean processPendingWrites() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "processPendingWrites");

        // Clean up the write queue of all finished writes that are complete (successfully or failed for good)
        val cleanupResult = this.writes.removeFinishedWrites();
        if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.WriteFailed) {
            // We encountered a failed write. As such, we must close immediately and not process anything else.
            // Closing will automatically cancel all pending writes.
            close();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, cleanupResult);
            return false;
        } else {
            if (cleanupResult.getRemovedCount() > 0) {
                this.queueStateChangeListeners.notifySourceChanged();
            }

            if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.QueueEmpty) {
                // Queue is empty - nothing else to do.
                LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, cleanupResult);
                return true;
            }
        }

        // Get the writes to execute from the queue.
        List<Write> toExecute = getWritesToExecute();

        // Execute the writes, if any.
        boolean success = true;
        if (!toExecute.isEmpty()) {
            success = executeWrites(toExecute);

            if (success) {
                // After every run where we did write, check if need to trigger a rollover.
                this.rolloverProcessor.runAsync();
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, toExecute.size(), success);
        return success;
    }

    /**
     * Collects an ordered list of Writes to execute to FileSystem.
     *
     * @return The list of Writes to execute.
     */
    private List<Write> getWritesToExecute() {
        // Calculate how much estimated space there is in the current file.
        final long maxTotalSize = this.config.getFsFileMaxSize() - getWriteFile().file.toFile().length();

        // Get the writes to execute from the queue.
        List<Write> toExecute = this.writes.getWritesToExecute(maxTotalSize);

        // Check to see if any writes executed on closed files, in which case they either need to be failed (if deemed
        // appropriate, or retried).
        if (handleClosedFiles(toExecute)) {
            // If any changes were made to the Writes in the list, re-do the search to get a more accurate list of Writes
            // to execute (since some may have changed Files, more writes may not be eligible for execution).
            toExecute = this.writes.getWritesToExecute(maxTotalSize);
        }

        return toExecute;
    }

    /**
     * Executes the given Writes to FileSystem.
     *
     * @param toExecute The Writes to execute.
     * @return True if all the writes succeeded, false if at least one failed (if a Write failed, all subsequent writes
     * will be failed as well).
     */
    private boolean executeWrites(List<Write> toExecute) {
        log.debug("{}: Executing {} writes.", this.traceObjectId, toExecute.size());
        for (int i = 0; i < toExecute.size(); i++) {
            Write w = toExecute.get(i);
            try {
                // Record the beginning of a new attempt.
                int attemptCount = w.beginAttempt();
                if (attemptCount > this.config.getFsMaxWriteAttempts()) {
                    // Retried too many times.
                    throw new RetriesExhaustedException(w.getFailureCause());
                }

                // Invoke the FileSystem write.
                Throwable error = null;
                long addConfirmedEntryId = -1L;
                FileLock lock = null;
                try {
                    RandomAccessFile raf = w.getWriteFile().raf;
                    try {
                        lock = raf.getChannel().tryLock();
                        if (lock != null) {
                            raf.seek(0);
                            while (raf.readLine() != null) {
                                addConfirmedEntryId++;
                            }
                            raf.write(ByteBufUtil.getBytes(w.getData().retain()));
                            raf.write(System.getProperty("line.separator").getBytes());
                            addConfirmedEntryId++;
                        }
                    } finally {
                        if (lock != null) {
                            lock.release();
                        }
                    }
                } catch (Throwable ex) {
                    error = ex;
                }
                addCallback(addConfirmedEntryId, error, w);
            } catch (Throwable ex) {
                // Synchronous failure (or RetriesExhausted). Fail current write.
                boolean isFinal = !isRetryable(ex);
                w.fail(ex, isFinal);

                // And fail all remaining writes as well.
                for (int j = i + 1; j < toExecute.size(); j++) {
                    toExecute.get(j).fail(new DurableDataLogException("Previous write failed.", ex), isFinal);
                }

                return false;
            }
        }

        // Success.
        return true;
    }

    /**
     * Checks each Write in the given list if it is pointing to a closed WriteFile. If so, it verifies if the write has
     * actually been committed (in case we hadn't been able to determine its outcome) and updates the file, if needed.
     *
     * @param writes An ordered list of Writes to inspect and update.
     * @return True if any of the Writes in the given list has been modified (either completed or had its WriteFile
     * changed).
     */
    private boolean handleClosedFiles(List<Write> writes) {
        if (writes.size() == 0 || !writes.get(0).getWriteFile().isClosed()) {
            // Nothing to do. We only need to check the first write since, if a write failed with FileClosed, then the
            // first write must have failed for that reason (a file is closed implies all files before it are closed too).
            return false;
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "handleClosedFiles", writes.size());
        WriteFile currentFile = getWriteFile();
        Map<Path, Long> lastAddsConfirmed = new HashMap<>();
        boolean anythingChanged = false;
        for (Write w : writes) {
            if (w.isDone() || !w.getWriteFile().isClosed()) {
                continue;
            }

            // Write likely failed because of FileClosedException. Need to check the LastAddConfirmed for each
            // involved File and see if the write actually made it through or not.
            long lac = fetchLastAddConfirmed(w.getWriteFile(), lastAddsConfirmed);
            if (w.getEntryId() >= 0 && w.getEntryId() <= lac) {
                // Write was actually successful. Complete it and move on.
                completeWrite(w);
                anythingChanged = true;
            } else if (currentFile.file.compareTo(w.getWriteFile().file) != 0) {
                // Current file has changed; attempt to write to the new one.
                w.setWriteFile(currentFile);
                anythingChanged = true;
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "handleClosedFiles", traceId, writes.size(), anythingChanged);
        return anythingChanged;
    }

    /**
     * Reliably gets the LastAddConfirmed for the WriteFiles
     *
     * @param writeFile         The writeFile to query.
     * @param lastAddsConfirmed A Map of file path to LastAddConfirmed for each known file path. This is used as a cache
     *                          and will be updated if necessary.
     * @return The LastAddConfirmed for the WriteFile.
     */
    @SneakyThrows(DurableDataLogException.class)
    private long fetchLastAddConfirmed(WriteFile writeFile, Map<Path, Long> lastAddsConfirmed) {
        Path file = writeFile.file;
        long lac = lastAddsConfirmed.getOrDefault(file, -1L);
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "fetchLastAddConfirmed", file, lac);
        if (lac < 0) {
            FileLock lock = null;
            try {
                RandomAccessFile raf = writeFile.raf;
                try {
                    lock = raf.getChannel().lock(0, Long.MAX_VALUE, true);
                    if (lock != null) {
                        raf.seek(0);
                        while (raf.readLine() != null) {
                            lac++;
                        }
                    }
                } finally {
                    if (lock != null) {
                        lock.release();
                    }
                }
            } catch (Exception ex) {
                lac = FileLogs.NO_ENTRY_ID;
            }
            lastAddsConfirmed.put(file, lac);
            log.info("{}: Fetched actual LastAddConfirmed ({}) for file {}.", this.traceObjectId, lac, file);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "fetchLastAddConfirmed", traceId, file, lac);
        return lac;
    }

    /**
     * Callback for FileSystemLog appends.
     *
     * @param entryId Assigned EntryId.
     * @param error   Error.
     * @param write   the Write we were writing.
     */
    private void addCallback(Long entryId, Throwable error, Write write) {
        try {
            if (error == null) {
                assert entryId != null;
                write.setEntryId(entryId);
                // Successful write. If we get this, then by virtue of how the Writes are executed, it is safe to complete the callback future now.
                completeWrite(write);
                return;
            }

            // Convert the response code into an Exception. Eventually this will be picked up by the WriteProcessor which
            // will retry it or fail it permanently (this includes exceptions from rollovers).
            handleWriteException(error, write);
        } catch (Throwable ex) {
            // Most likely a bug in our code. We still need to fail the write so we don't leave it hanging.
            write.fail(ex, !isRetryable(ex));
        } finally {
            // Process all the appends in the queue after any change. This finalizes the completion, does retries (if needed)
            // and triggers more appends.
            try {
                this.writeProcessor.runAsync();
            } catch (ObjectClosedException ex) {
                // In case of failures, the WriteProcessor may already be closed. We don't want the exception to propagate
                // to FileSystem.
                log.warn("{}: Not running WriteProcessor as part of callback due to FileSystemLog being closed.", this.traceObjectId, ex);
            }
        }
    }

    /**
     * Completes the given Write and makes any necessary internal updates.
     *
     * @param write The write to complete.
     */
    private void completeWrite(Write write) {
        Timer t = write.complete();
        if (t != null) {
            this.metrics.fileSystemWriteCompleted(write.getLength(), t.getElapsed());
        }
    }

    /**
     * Handles a general Write exception.
     */
    private void handleWriteException(Throwable ex) {
        if (ex instanceof ObjectClosedException && !this.closed.get()) {
            log.warn("{}: Caught ObjectClosedException but not closed; closing now.", this.traceObjectId, ex);
            close();
        }
    }

    /**
     * Handles an exception after a Write operation, converts it to a Pravega Exception and completes the given future
     * exceptionally using it.
     *
     * @param ex The exception from File System.
     * @param write The Write that failed.
     */
    @VisibleForTesting
    static void handleWriteException(Throwable ex, Write write) {
        if (ex instanceof OverlappingFileLockException) {
            // We were fenced out.
            ex = new DataLogWriterNotPrimaryException("FileSystemLog is not primary anymore.", ex);
        } else if (ex instanceof IOException) {
            ex = new WriteFailureException("Unable to write to active File.", ex);
        } else {
            // All the other kind of exceptions go in the same bucket.
            ex = new DurableDataLogException("General exception while accessing FileSystem.", ex);
        }
        write.fail(ex, !isRetryable(ex));
    }

    /**
     * Determines whether the given exception can be retried.
     */
    private static boolean isRetryable(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return ex instanceof WriteFailureException;
    }

    /**
     * Attempts to truncate the Log. The general steps are:
     * 1. Create an in-memory copy of the metadata reflecting the truncation.
     * 2. Attempt to persist the metadata to ZooKeeper.
     * 2.1. This is the only operation that can fail the process. If this fails, the operation stops here.
     * 3. Swap in-memory metadata pointers.
     * 4. Delete truncated-out files.
     * 4.1. If any of the files cannot be deleted, no further attempt to clean them up is done.
     *
     * @param upToAddress The address up to which to truncate.
     */
    @SneakyThrows(DurableDataLogException.class)
    private void tryTruncate(FileAddress upToAddress) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "tryTruncate", upToAddress);

        // Truncate the metadata and get a new copy of it.
        val oldMetadata = getLogMetadata();
        val newMetadata = oldMetadata.truncate(upToAddress);

        // Attempt to persist the new Log Metadata. We need to do this first because if we delete the files but were
        // unable to update the metadata, then the log will be corrupted (metadata points to inexistent files).
        persistMetadata(newMetadata, false);

        // Repoint our metadata to the new one.
        synchronized (this.lock) {
            this.logMetadata = newMetadata;
        }

        // Determine files to delete and delete them.
        val filesToKeep = newMetadata.getFiles().stream().map(FileMetadata::getFile).collect(Collectors.toSet());
        val filesDelete = oldMetadata.getFiles().stream().filter(fm -> !filesToKeep.contains(fm.getFile())).iterator();
        while (filesDelete.hasNext()) {
            val fm = filesDelete.next();
            try {
                FileLogs.delete(fm.getFile());
            } catch (DurableDataLogException ex) {
                // Nothing we can do if we can't delete a file; we've already updated the metadata. Log the error and
                // move on.
                log.error("{}: Unable to delete truncated file {}.", this.traceObjectId, fm.getFile(), ex);
            }
        }

        log.info("{}: Truncated up to {}.", this.traceObjectId, upToAddress);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "tryTruncate", traceId, upToAddress);
    }

    //endregion

    //region Metadata Management

    /**
     * Loads the metadata for the current log, as stored in ZooKeeper.
     *
     * @return A new LogMetadata object with the desired information, or null if no such node exists.
     * @throws DataLogInitializationException If an Exception (other than NoNodeException) occurred.
     */
    @VisibleForTesting
    LogMetadata loadMetadata() throws DataLogInitializationException {
        try {
            Stat storingStatIn = new Stat();
            byte[] serializedMetadata = this.zkClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.SERIALIZER.deserialize(serializedMetadata);
            result.withUpdateVersion(storingStatIn.getVersion());
            return result;
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("{}: No ZNode found for path '{}{}'. This is OK if this is the first time accessing this log.",
                    this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath);
            return null;
        } catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to load ZNode contents for path '%s%s'.",
                    this.zkClient.getNamespace(), this.logNodePath), ex);
        }
    }

    /**
     * Updates the metadata and persists it as a result of adding a new file.
     *
     * @param currentMetadata   The current metadata.
     * @param newFile           The newly added file.
     * @param clearEmptyFiles   If true, the new metadata will not contain any pointers to empty files. Setting this
     *                          to true will not remove a pointer to the last few files in the Log (controlled by
     *                          Files.MIN_OPEN_FILE_COUNT), even if they are indeed empty (this is so we don't interfere
     *                          with any ongoing opening activities as another instance of this Log may not have yet been
     *                          opened).
     * @return A new instance of the LogMetadata, which includes the new file.
     * @throws DurableDataLogException If an Exception occurred.
     */
    private LogMetadata updateMetadata(LogMetadata currentMetadata, Path newFile, boolean clearEmptyFiles) throws DurableDataLogException {
        boolean create = currentMetadata == null;
        if (create) {
            // This is the first file ever in the metadata.
            currentMetadata = new LogMetadata(newFile);
        } else {
            currentMetadata = currentMetadata.addFile(newFile);
            if (clearEmptyFiles) {
                // Remove those files from the metadata that are empty.
                currentMetadata = currentMetadata.removeEmptyFiles(FileLogs.MIN_OPEN_FILE_COUNT);
            }
        }

        try {
            persistMetadata(currentMetadata, create);
        } catch (DataLogWriterNotPrimaryException ex) {
            // Only attempt to clean up the newly created file if we were fenced out. Any other exception is not indicative
            // of whether we were able to persist the metadata or not, so it's safer to leave the file behind in case
            // it is still used. If indeed our metadata has been updated, a subsequent recovery will pick it up and delete it
            // because it (should be) empty.
            try {
                FileLogs.delete(newFile);
            } catch (Exception deleteEx) {
                log.warn("{}: Unable to delete newly created file {}.", this.traceObjectId, newFile, deleteEx);
                ex.addSuppressed(deleteEx);
            }

            throw ex;
        } catch (Exception ex) {
            log.warn("{}: Error while using ZooKeeper. Leaving orphaned file {} behind.", this.traceObjectId, newFile);
            throw ex;
        }

        log.info("{} Metadata updated ({}).", this.traceObjectId, currentMetadata);
        return currentMetadata;
    }

    /**
     * Persists the given metadata into ZooKeeper.
     *
     * @param metadata The LogMetadata to persist. At the end of this method, this metadata will have its Version updated
     *                 to the one in ZooKeeper.
     * @param create   Whether to create (true) or update (false) the data in ZooKeeper.
     * @throws DataLogWriterNotPrimaryException If the metadata update failed (if we were asked to create and the node
     *                                          already exists or if we had to update and there was a version mismatch).
     * @throws DurableDataLogException          If another kind of exception occurred.
     */
    private void persistMetadata(LogMetadata metadata, boolean create) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize(metadata).getCopy();
            Stat result = create
                    ? createZkMetadata(serializedMetadata)
                    : updateZkMetadata(serializedMetadata, metadata.getUpdateVersion());
            metadata.withUpdateVersion(result.getVersion());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            if (reconcileMetadata(metadata)) {
                log.info("{}: Received '{}' from ZooKeeper while persisting metadata (path = '{}{}'), however metadata has been persisted correctly. Not rethrowing.",
                        this.traceObjectId, keeperEx, this.zkClient.getNamespace(), this.logNodePath);
            } else {
                // We were fenced out. Convert to an appropriate exception.
                throw new DataLogWriterNotPrimaryException(
                        String.format("Unable to acquire exclusive write lock for log (path = '%s%s').", this.zkClient.getNamespace(), this.logNodePath),
                        keeperEx);
            }
        } catch (Exception generalEx) {
            // General exception. Convert to an appropriate exception.
            throw new DataLogInitializationException(
                    String.format("Unable to update ZNode for path '%s%s'.", this.zkClient.getNamespace(), this.logNodePath),
                    generalEx);
        }

        log.info("{} Metadata persisted ({}).", this.traceObjectId, metadata);
    }

    @VisibleForTesting
    protected Stat createZkMetadata(byte[] serializedMetadata) throws Exception {
        val result = new Stat();
        this.zkClient.create().creatingParentsIfNeeded().storingStatIn(result).forPath(this.logNodePath, serializedMetadata);
        return result;
    }

    @VisibleForTesting
    protected Stat updateZkMetadata(byte[] serializedMetadata, int version) throws Exception {
        return this.zkClient.setData().withVersion(version).forPath(this.logNodePath, serializedMetadata);
    }

    /**
     * Verifies the given {@link LogMetadata} against the actual one stored in ZooKeeper.
     *
     * @param metadata The Metadata to check.
     * @return True if the metadata stored in ZooKeeper is an identical match to the given one, false otherwise. If true,
     * {@link LogMetadata#getUpdateVersion()} will also be updated with the one stored in ZooKeeper.
     */
    private boolean reconcileMetadata(LogMetadata metadata) {
        try {
            val actualMetadata = loadMetadata();
            if (metadata.equals(actualMetadata)) {
                metadata.withUpdateVersion(actualMetadata.getUpdateVersion());
                return true;
            }
        } catch (DataLogInitializationException ex) {
            log.warn("{}: Unable to verify persisted metadata (path = '{}{}').", this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath, ex);
        }
        return false;
    }

    //endregion

    //region Ledger Rollover

    /**
     * Triggers an asynchronous rollover, if the current Write File has exceeded its maximum length.
     * The rollover protocol is as follows:
     * 1. Create a new file.
     * 2. Create an in-memory copy of the metadata and add the new file to it.
     * 3. Update the metadata in ZooKeeper using compare-and-set.
     * 3.1 If the update fails, the newly created file is deleted and the operation stops.
     * 4. Swap in-memory pointers to the active Write File (all future writes will go to the new file).
     * 5. Close the previous file.
     * 5.1 If closing fails, there is nothing we can do. We've already opened a new file and new writes are going to it.
     *
     * NOTE: this method is not thread safe and is not meant to be executed concurrently. It should only be invoked as
     * part of the Rollover Processor.
     */
    @SneakyThrows(DurableDataLogException.class) // Because this is an arg to SequentialAsyncProcessor, which wants a Runnable.
    private void rollover() {
        if (this.closed.get()) {
            // FileSystemLog is closed; no point in running this.
            return;
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollover");
        val f = getWriteFile();
        if (!f.isClosed() && f.file.toFile().length() < this.config.getFsFileMaxSize()) {
            // Nothing to do. Trigger the write processor just in case this rollover was invoked because the write
            // processor got a pointer to a WriteFile that was just closed by a previous run of the rollover processor.
            this.writeProcessor.runAsync();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, false);
            return;
        }

        try {
            // Create new file.
            Path newPath = FileLogs.create(this.logId);
            log.debug("{}: Rollover: created new file {}.", this.traceObjectId, newPath);

            // Update the metadata.
            LogMetadata metadata = getLogMetadata();
            metadata = updateMetadata(metadata, newPath, false);
            FileMetadata fileMetadata = metadata.getFile(newPath);
            assert fileMetadata != null : "cannot find newly added file metadata";
            log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata);

            // Update pointers to the new file and metadata.
            WriteFile oldFile;
            synchronized (this.lock) {
                oldFile = this.writeFile;
                if (!oldFile.isClosed()) {
                    // Only mark the old file as Rolled Over if it is still open. Otherwise it means it was closed
                    // because of some failure and should not be marked as such.
                    this.writeFile.setRolledOver(true);
                }

                RandomAccessFile newRaf = FileLogs.openWrite(newPath);
                this.writeFile = new WriteFile(newPath, newRaf, fileMetadata);
                this.logMetadata = metadata;
            }

            // Close the old file. This must be done outside of the lock, otherwise the pending writes (and their callbacks)
            // will be invoked within the lock, thus likely candidates for deadlocks.
            oldFile.setClosed(true);
            FileLogs.close(oldFile.raf, oldFile.file);
            log.info("{}: Rollover: swapped file and metadata pointers (Old = {}, New = {}) and closed old file.",
                    this.traceObjectId, oldFile.file, newPath);
        } finally {
            // It's possible that we have writes in the queue that didn't get picked up because they exceeded the predicted
            // file length. Invoke the Write Processor to execute them.
            this.writeProcessor.runAsync();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, true);
        }
    }

    /**
     * Determines which files are safe to delete from FileSystem.
     *
     * @param oldMetadata     A pointer to the previous version of the metadata, that contains all files eligible for
     *                        deletion. Only those files that do not exist in currentMetadata will be selected.
     * @param currentMetadata A pointer to the current version of the metadata. No file that is referenced here will
     *                        be selected.
     * @return A List that contains file paths to remove. May be empty.
     */
    @GuardedBy("lock")
    private List<Path> getFilesToDelete(LogMetadata oldMetadata, LogMetadata currentMetadata) {
        if (oldMetadata == null) {
            return Collections.emptyList();
        }

        val existingFiles = currentMetadata.getFiles().stream()
                .map(FileMetadata::getFile)
                .collect(Collectors.toSet());
        return oldMetadata.getFiles().stream()
                .map(FileMetadata::getFile)
                .filter(file -> !existingFiles.contains(file))
                .collect(Collectors.toList());
    }

    //endregion

    //region Helpers

    private void reportMetrics() {
        LogMetadata metadata = getLogMetadata();
        if (metadata != null) {
            this.metrics.fileCount(metadata.getFiles().size());
            this.metrics.queueStats(this.writes.getStatistics());
        }
    }


    private LogMetadata getLogMetadata() {
        synchronized (this.lock) {
            return this.logMetadata;
        }
    }

    private WriteFile getWriteFile() {
        synchronized (this.lock) {
            return this.writeFile;
        }
    }

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.lock) {
            Preconditions.checkState(this.writeFile != null, "FileSystemLog is not initialized.");
            assert this.logMetadata != null : "writeFile != null but logMetadata == null";
        }
    }

    //endregion
}
