package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferedIterator;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DataLogCorruptedException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Performs read from FileSystem Logs.
 */
@Slf4j
@NotThreadSafe
public class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
    //region Members

    private final int logId;
    private final LogMetadata metadata;
    private final AtomicBoolean closed;
    private final FileSystemConfig config;
    private ReadFile currentFile;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogReader class.
     *
     * @param logId      The Id of the {@link FileSystemLog} to read from. This is used for validation purposes.
     * @param metadata   The LogMetadata of the Log to read.
     * @param config     Configuration to use.
     */
    LogReader(int logId, LogMetadata metadata, FileSystemConfig config) {
        this.logId = logId;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.config = Preconditions.checkNotNull(config, "config");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.currentFile != null) {
                this.currentFile.close();
                this.currentFile = null;
            }
        }
    }

    @Override
    public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (this.currentFile == null) {
            // First time we call this. Locate the first file based on the metadata truncation address. We don't know
            // how many entries are in that first file, so open it anyway so we can figure out.
            openNextFile(this.metadata.getNextAddress(this.metadata.getTruncationAddress(), Long.MAX_VALUE));
        }

        while (this.currentFile != null && (!this.currentFile.canRead())) {
            // We have reached the end of the current file. Find next one, and skip over empty files).
            long lastEntryId = FileLogs.getLastAddConfirmed(this.currentFile.raf, this.currentFile.file);
            val lastAddress = new FileAddress(this.currentFile.metadata, lastEntryId);
            this.currentFile.close();
            openNextFile(this.metadata.getNextAddress(lastAddress, lastEntryId));
        }

        // Try to read from the current file.
        if (this.currentFile == null || this.currentFile.isEmpty()) {
            return null;
        }

        return wrapItem(this.currentFile.reader.next(), this.currentFile.metadata);
    }

    private void openNextFile(FileAddress address) throws DurableDataLogException {
        if (address == null) {
            // We have reached the end.
            close();
            return;
        }

        FileMetadata metadata = this.metadata.getFile(address.getFile());
        assert metadata != null : "no FileMetadata could be found with valid fileAddress " + address;

        // Open the file.
        Path file = metadata.getFile();
        RandomAccessFile raf = FileLogs.openRead(file);

        checkLogIdProperty(file);

        try {
            long lastEntryId = FileLogs.getLastAddConfirmed(raf, file);
            if (lastEntryId < address.getEntryId()) {
                // This file is empty.
                this.currentFile = ReadFile.empty(metadata, file, raf);
                return;
            }

            ReadFile previousFile;
            previousFile = this.currentFile;
            this.currentFile = new ReadFile(metadata, file, raf, address.getEntryId(), lastEntryId, this.config.getFsReadBatchSize());
            if (previousFile != null) {
                // Close previous read file.
                previousFile.close();
            }
        } catch (Exception ex) {
            FileLogs.close(raf, file);
            close();
            throw new DurableDataLogException("Error while reading from FileSystem.", ex);
        }
    }

    private void checkLogIdProperty(Path file) throws DataLogCorruptedException {
        int actualLogId = FileLogs.getFileSystemLogId(file);
        // For special log repair operations, we allow the LogReader to read from a special log id that contains admin-provided
        // changes to repair the original lod data (i.e., FileLogs.REPAIR_LOG_ID).
        if (actualLogId != FileLogs.NO_LOG_ID && actualLogId != this.logId && actualLogId != FileLogs.REPAIR_LOG_ID) {
            throw new DataLogCorruptedException(String.format("FileSystemLog %s contains file %s which belongs to FileSystemLog %s.",
                    this.logId, file, actualLogId));
        }
    }

    //endregion

    //region ReadItem

    private static class ReadItem implements DurableDataLog.ReadItem {
        @Getter
        private final InputStream payload;
        @Getter
        private final int length;
        @Getter
        private final FileAddress address;

        ReadItem(long entryId, InputStream payload, int length, FileMetadata fileMetadata) {
            this.address = new FileAddress(fileMetadata, entryId);
            this.payload = payload;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    private static DurableDataLog.ReadItem wrapItem(ReadEntry entry, FileMetadata metadata) {
        ByteBuf content = Unpooled.wrappedBuffer(entry.getData());
        return new ReadItem(entry.getEntryId(),
                new ByteBufInputStream(content, false /*relaseOnClose*/),
                content.readableBytes(), metadata);
    }

    //endregion

    //region ReadEntry

    private static class ReadEntry {
        @Getter
        final byte[] data;
        @Getter
        final long entryId;

        public ReadEntry(byte[] data, long entryId) {
            this.data = data;
            this.entryId = entryId;
        }
    }

    //endregion

    //region ReadFile

    private static class ReadFile {
        final FileMetadata metadata;
        final Path file;
        final RandomAccessFile raf;
        final BufferedIterator<ReadEntry> reader;
        final AtomicBoolean closed = new AtomicBoolean(false);
        volatile List<ReadEntry> currentFileEntries;

        public ReadFile(FileMetadata metadata, Path file, RandomAccessFile raf, long firstEntryId, long lastEntryId, int batchSize) {
            this.metadata = metadata;
            this.file = file;
            this.raf = raf;
            if (lastEntryId >= firstEntryId) {
                this.reader = new BufferedIterator<>(this::readRange, firstEntryId, lastEntryId, batchSize);
            } else {
                // Empty file;
                this.reader = null;
            }
            this.currentFileEntries = new ArrayList<>();
        }

        boolean isEmpty() {
            return this.reader == null;
        }

        private void close() {
            // Release memory held by FileSystemLog internals.
            // we have to prevent a double free
            if (closed.compareAndSet(false, true)) {
                if (!currentFileEntries.isEmpty()) {
                    currentFileEntries.clear();
                }
                // closing a file is mostly a no-op, it is not expected
                // to really fail
                try {
                    FileLogs.close(raf, file);
                } catch (DurableDataLogException ex) {
                    log.error("Unable to close File {}.", file, ex);
                }
            }
        }

        @SneakyThrows(IOException.class)
        private Iterator<ReadEntry> readRange(long fromEntryId, long toEntryId) {
            if (!currentFileEntries.isEmpty()) {
                currentFileEntries.clear();
            }
            FileLock lock = null;
            try {
                lock = raf.getChannel().lock(0, Long.MAX_VALUE, true);
                if (lock != null) {
                    raf.seek(0);
                    long entryId = -1;
                    String entry;
                    while ((entry = raf.readLine()) != null && ++entryId <= toEntryId) {
                        if (entryId >= fromEntryId) {
                            currentFileEntries.add(new ReadEntry(entry.getBytes(), entryId));
                        }
                    }
                }
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
            return currentFileEntries.iterator();
        }

        static ReadFile empty(@NonNull FileMetadata metadata, @NonNull Path file, @NonNull RandomAccessFile raf) {
            return new ReadFile(metadata, file, raf, Long.MAX_VALUE, Long.MIN_VALUE, 1);
        }

        boolean canRead() {
            return this.reader != null && this.reader.hasNext();
        }
    }

    //endregion
}
