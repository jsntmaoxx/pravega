package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;

import java.util.List;

/**
 * Defines a read-only view of the FileSystem Log Metadata.
 */
public interface ReadOnlyFileSystemLogMetadata extends ReadOnlyLogMetadata {
    /**
     * Gets a read-only ordered list of FileMetadata instances representing the Files that currently make up this
     * Log Metadata.
     *
     * @return A new read-only list.
     */
    List<FileMetadata> getFiles();

    /**
     * Gets a FileAddress representing the first location in the log that is accessible for reads.
     *
     * @return The Truncation Address.
     */
    FileAddress getTruncationAddress();

    /**
     * Determines whether this {@link ReadOnlyFileSystemLogMetadata} is equivalent to the other one.
     *
     * @param other The other instance.
     * @return True if equivalent, false otherwise.
     */
    default boolean equals(ReadOnlyFileSystemLogMetadata other) {
        if (other == null) {
            return false;
        }

        List<FileMetadata> files = getFiles();
        List<FileMetadata> otherFiles = other.getFiles();
        if (this.isEnabled() != other.isEnabled()
                || this.getEpoch() != other.getEpoch()
                || !this.getTruncationAddress().equals(other.getTruncationAddress())
                || files.size() != otherFiles.size()) {
            return false;
        }

        // Check each file.
        for (int i = 0; i < files.size(); i++) {
            if (!files.get(i).equals(otherFiles.get(i))) {
                return false;
            }
        }

        // All tests have passed.
        return true;
    }
}
