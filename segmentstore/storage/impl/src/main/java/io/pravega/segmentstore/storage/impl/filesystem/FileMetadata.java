package io.pravega.segmentstore.storage.impl.filesystem;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;

/**
 * Represents metadata about a particular file.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public class FileMetadata {
    /**
     * The path of the file in FileSystem.
     */
    private final Path file;

    /**
     * The metadata-assigned internal sequence number of the file inside the log.
     */
    private final int sequence;

    /**
     * Gets the current status of this file.
     */
    private final Status status;

    /**
     * Creates a new instance of the FileMetadata class with an unknown Empty Status.
     *
     * @param file     The path of the file.
     * @param sequence The metadata-assigned sequence number.
     */
    FileMetadata(Path file, int sequence) {
        this(file, sequence, Status.Unknown);
    }

    @Override
    public String toString() {
        return String.format("file = %s, Sequence = %d, Status = %s", this.file, this.sequence, this.status);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    enum Status {
        Unknown((byte) 0),
        Empty((byte) 1),
        NotEmpty((byte) 2);
        @Getter
        private final byte value;

        static Status valueOf(byte b) {
            if (b == Unknown.value) {
                return Unknown;
            } else if (b == Empty.value) {
                return Empty;
            } else if (b == NotEmpty.value) {
                return NotEmpty;
            }

            throw new IllegalArgumentException("Unsupported Status " + b);
        }
    }
}
