package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DurableDataLogException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * General utilities pertaining to FileSystem Files.
 */
@Slf4j
public class FileLogs {
    static final int NO_LOG_ID = -1; // INVALID_LOG_ID
    static final long NO_ENTRY_ID = -1; // INVALID_ENTRY_ID

    /**
     * For special log repair operations, we allow the LogReader to read from a special log id that contains
     * admin-provided changes to repair the original lod data.
     */
    static final int REPAIR_LOG_ID = Integer.MAX_VALUE;

    /**
     * The root dir of file logs.
     */
    static final String ROOT_DIR = "/home/maox2/Documents/logs";

    /**
     * How many files to open (from the end of the list) when acquiring lock.
     */
    static final int MIN_OPEN_FILE_COUNT = 2;

    /**
     * Creates a new file in FileSystem.
     * @param logId The Id of the {@link FileSystemLog} that owns this files. This will be codified in the new
     *              file's metadata so that it may be recovered in case we need to reconstruct the {@link FileSystemLog}
     *              in the future.
     *
     * @return A Path for the new file.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static Path create(int logId) throws DurableDataLogException {
        File logDir = Paths.get(ROOT_DIR, String.valueOf(logId)).toFile();
        if (!logDir.exists() && !logDir.mkdirs()) {
            throw new DurableDataLogException("Unable to create new FileSystemLog file.");
        }

        Path file = Paths.get(logDir.getPath(), String.valueOf(logDir.listFiles().length));
        try {
            if (!file.toFile().exists()) {
                file.toFile().createNewFile();
            }
            return file;
        } catch (IOException ex) {
            throw new DurableDataLogException(String.format("Unable to create file %s.", file), ex);
        }
    }

    /**
     * Opens a file for read.
     *
     * @param file   The path of the file to open.
     * @return A RandomAccessFile for the newly opened file.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static RandomAccessFile openRead(Path file) throws DurableDataLogException {
        try {
            return Exceptions.handleInterruptedCall(
                    () -> new RandomAccessFile(file.toFile(), "r"));
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to open-read file %s.", file), ex);
        }
    }

    /**
     * Opens a file for write.
     *
     * @param file   The path of the file to open.
     * @return A RandomAccessFile for the newly opened file.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static RandomAccessFile openWrite(Path file) throws DurableDataLogException {
        try {
            return Exceptions.handleInterruptedCall(
                    () -> new RandomAccessFile(file.toFile(), "rw"));
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to open-write file %s.", file), ex);
        }
    }

    /**
     * Closes the given File.
     *
     * @param raf   The RandomAccessFile to close.
     * @param file  The path of the file to close.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void close(RandomAccessFile raf, Path file) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(raf::close);
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to close file %s.", file), ex);
        }
    }

    /**
     * Deletes the file with given path.
     *
     * @param file   The path of the file to delete.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void delete(Path file) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(
                    () -> {
                        if (file.toFile().exists()) {
                            file.toFile().delete();
                        }
                    });
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to delete file %s.", file), ex);
        }
    }

    /**
     * Open a Log made up of the given files.
     *
     * @param files         An ordered list of FileMetadata objects representing all the files in the log.
     * @param traceObjectId Used for logging.
     * @return A Map of FilePath to LastAddConfirmed for those files that were opened and had a different
     * LastAddConfirmed than what their FileMetadata was indicating.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static Map<Path, Long> open(List<FileMetadata> files, String traceObjectId) throws DurableDataLogException {
        // Open the files, in descending order. During the process, we need to determine whether the files we
        // open actually have any data in them, and update the FileMetadata accordingly.
        // We need to open at least MIN_OPEN_FILE_COUNT files that are not empty to properly ensure we opened
        // the log correctly and identify any empty files (Since this algorithm is executed upon every recovery, any
        // empty files should be towards the end of the Log).
        int nonEmptyCount = 0;
        val result = new HashMap<Path, Long>();
        val iterator = files.listIterator(files.size());
        while (iterator.hasPrevious() && (nonEmptyCount < MIN_OPEN_FILE_COUNT)) {
            FileMetadata fileMetadata = iterator.previous();
            RandomAccessFile raf;
            long lastAddConfirmed;
            try {
                raf = openRead(fileMetadata.getFile());
                lastAddConfirmed = getLastAddConfirmed(raf, fileMetadata.getFile());
            } catch (DurableDataLogException ex) {
                val c = ex.getCause();
                if (fileMetadata.getStatus() == FileMetadata.Status.Empty && (c instanceof FileNotFoundException)) {
                    log.warn("{}: Unable to open EMPTY file {}. Skipping.", traceObjectId, fileMetadata, ex);
                    continue;
                }
                throw ex;
            }

            if (lastAddConfirmed != NO_ENTRY_ID) {
                // Non-empty.
                nonEmptyCount++;
            }

            if (fileMetadata.getStatus() == FileMetadata.Status.Unknown) {
                // We did not know the status of this file before, but now we do.
                result.put(fileMetadata.getFile(), lastAddConfirmed);
            }

            close(raf, fileMetadata.getFile());
            log.info("{}: Opened File {}.", traceObjectId, fileMetadata);
        }

        return result;
    }

    /**
     * Get the last confirmed entry id on this file.
     *
     * @param raf              The RandomAccessFile to query.
     * @param file             The path of the file to query.
     * @return The id of the last confirmed entry of the file.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    public static long getLastAddConfirmed(RandomAccessFile raf, Path file) throws DurableDataLogException {
        long addConfirmedEntryId = NO_ENTRY_ID;
        FileLock lock = null;
        try {
            try {
                lock = raf.getChannel().lock(0, Long.MAX_VALUE, true);
                if (lock != null) {
                    raf.seek(0);
                    while (raf.readLine() != null) {
                        addConfirmedEntryId++;
                    }
                }
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        } catch(IOException ex){
            throw new DurableDataLogException(String.format("I/O error occurs to file %s.", file), ex);
        }
        return addConfirmedEntryId;
    }

    /**
     * Gets the log id from the given {@link Path}, as stored in its file path.
     *
     * @param file The {@link Path} of the file to query.
     * @return The Log Id stored in {@link Path}, or {@link #NO_LOG_ID} if not defined (i.e., due to an upgrade
     * from a version that did not store this information).
     */
    public static int getFileSystemLogId(Path file) {
        String logId = null;
        if (file.toString().startsWith(ROOT_DIR)) {
            String logPath = file.toString().substring(ROOT_DIR.length());
            if (logPath.startsWith(File.separator)) {
                logPath = logPath.substring(File.separator.length());
                if (logPath.contains(File.separator)) {
                    logId = logPath.substring(0, logPath.indexOf(File.separator));
                }
            }
        }

        try {
            return Integer.parseInt(logId);
        } catch (NumberFormatException ex) {
            log.error("File {} has invalid log id '{}'. Returning default value.", file, logId);
            return NO_LOG_ID;
        }
    }
}
