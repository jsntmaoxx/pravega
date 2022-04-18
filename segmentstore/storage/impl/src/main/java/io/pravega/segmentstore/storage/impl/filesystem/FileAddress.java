package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.LogAddress;
import lombok.Getter;

import java.nio.file.Path;

/**
 * LogAddress for FileSystem. Wraps around FileSystem-specific addressing scheme without exposing such information to the outside.
 */
public class FileAddress extends LogAddress implements Comparable<FileAddress> {
    //region Members

    private static final long INT_MASK = 0xFFFFFFFFL;
    /**
     * The path for the file of this address.
     */
    @Getter
    private final Path file;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileAddress class.
     *
     * @param fileSequence The sequence of the file (This is different from the Entry Sequence).
     * @param file         The path of the file that this Address corresponds to.
     * @param entryId      The Entry Id inside the file that this Address corresponds to.
     */
    FileAddress(int fileSequence, Path file, long entryId) {
        this(calculateAppendSequence(fileSequence, entryId), file);
    }

    /**
     * Creates a new instance of the FileAddress class.
     *
     * @param metadata The FileMetadata for the file.
     * @param entryId  The Entry Id inside the file that this Address corresponds to.
     */
    FileAddress(FileMetadata metadata, long entryId) {
        this(calculateAppendSequence(metadata.getSequence(), entryId), metadata.getFile());
    }

    /**
     * Creates a new instance of the FileAddress class.
     *
     * @param addressSequence The sequence of the Address (This is different from the File Sequence).
     * @param file            The path of the file that this Address corresponds to.
     */
    FileAddress(long addressSequence, Path file) {
        super(addressSequence);
        Preconditions.checkArgument(file != null, "file must not be null.");
        this.file = file;
    }

    //endregion

    //region Properties

    /**
     * Gets a Sequence number identifying the file inside the log. This is different from getSequence (which identifies
     * a particular write inside the entire log.
     *
     * @return The result.
     */
    int getFileSequence() {
        return (int) (getSequence() >>> 32);
    }

    /**
     * Gets a value representing the FileSystem-assigned entry id of this address. This entry id is unique per file, but
     * is likely duplicated across files (since it grows sequentially from 0 in each file).
     *
     * @return The result.
     */
    long getEntryId() {
        return getSequence() & INT_MASK;
    }

    @Override
    public String toString() {
        return String.format("%s, file = %s, EntryId = %d", super.toString(), this.file, getEntryId());
    }

    /**
     * Calculates the globally-unique append sequence by combining the file sequence and the entry id.
     *
     * @param fileSequence   The File Sequence (in the log). This will make up the high-order 32 bits of the result.
     * @param entryId        The Entry Id inside the file. This will be interpreted as a 32-bit integer and will make
     *                       up the low-order 32 bits of the result.
     * @return The calculated value.
     */
    private static long calculateAppendSequence(int fileSequence, long entryId) {
        return ((long) fileSequence << 32) + (entryId & INT_MASK);
    }

    //endregion

    //region Comparable Implementation

    @Override
    public int hashCode() {
        return Long.hashCode(getSequence());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileAddress) {
            return this.compareTo((FileAddress) obj) == 0;
        }

        return false;
    }

    @Override
    public int compareTo(FileAddress address) {
        return Long.compare(getSequence(), address.getSequence());
    }
}
