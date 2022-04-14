package io.pravega.segmentstore.storage.impl.filesystem;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * RandomAccessFile-FileMetadata pair.
 */
@RequiredArgsConstructor
public class WriteFile {
    final Path file;
    final RandomAccessFile raf;
    final FileMetadata metadata;

    /**
     * Whether this File has been closed in a controlled way and rolled over into a new file. This value is not
     * serialized and hence it should only be relied upon on active (write) files, and not on recovered files.
     */
    @Getter
    @Setter
    private boolean rolledOver;

    @Getter
    @Setter
    private boolean closed;

    @SneakyThrows
    @Override
    public String toString() {
        return String.format("%s, Length = %d, Closed = %s", this.metadata, raf.length(), this.closed);
    }
}
