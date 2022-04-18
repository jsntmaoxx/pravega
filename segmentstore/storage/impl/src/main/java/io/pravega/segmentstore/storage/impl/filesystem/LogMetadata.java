package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.CollectionHelpers;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Metadata for a File-based log.
 */
public class LogMetadata implements ReadOnlyFileSystemLogMetadata {
    //region Members

    static final VersionedSerializer.WithBuilder<LogMetadata, LogMetadata.LogMetadataBuilder> SERIALIZER = new Serializer();

    /**
     * The initial epoch to use for the Log.
     */
    @VisibleForTesting
    static final long INITIAL_EPOCH = 1;

    /**
     * The initial version for the metadata (for an empty log). Every time the metadata is persisted, its version is incremented.
     */
    @VisibleForTesting
    static final int INITIAL_VERSION = -1;

    /**
     * Sequence number of the first file in the log.
     */
    @VisibleForTesting
    static final int INITIAL_FILE_SEQUENCE = 1;

    /**
     * A LogAddress to be used when the log is not truncated (initially).
     */
    @VisibleForTesting
    static final FileAddress INITIAL_TRUNCATION_ADDRESS = new FileAddress(INITIAL_FILE_SEQUENCE - 1, Paths.get(""), 0);

    /**
     * The current epoch of the metadata. The epoch is incremented upon every successful recovery (as opposed from version,
     * which is incremented every time the metadata is persisted).
     */
    @Getter
    private final long epoch;

    /**
     * Whether the Log described by this LogMetadata is enabled or not.
     */
    @Getter
    private final boolean enabled;

    /**
     * An ordered list of FileMetadata instances that represent the files in the log.
     */
    @Getter
    private final List<FileMetadata> files;

    /**
     * The Address of the last write that was truncated out of the log. Every read will start from the next element.
     */
    @Getter
    private final FileAddress truncationAddress;
    private final AtomicInteger updateVersion;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogMetadata class with one file and epoch set to the default value.
     *
     * @param initialFile The path of the file to start the log with.
     */
    LogMetadata(Path initialFile) {
        this(INITIAL_EPOCH, true, Collections.singletonList(new FileMetadata(initialFile, INITIAL_FILE_SEQUENCE)),
                INITIAL_TRUNCATION_ADDRESS, INITIAL_VERSION);
    }

    /**
     * Creates a new instance of the LogMetadata class.
     *
     * @param epoch             The current Log epoch.
     * @param enabled           Whether this Log is enabled or not.
     * @param files             The ordered list of files making up this log.
     * @param truncationAddress The truncation address for this log. This is the address of the last entry that has been
     *                          truncated out of the log.
     * @param updateVersion     The Update version to set on this instance.
     */
    @Builder
    private LogMetadata(long epoch, boolean enabled, List<FileMetadata> files, FileAddress truncationAddress, int updateVersion) {
        Preconditions.checkArgument(epoch > 0, "epoch must be a positive number");
        this.epoch = epoch;
        this.enabled = enabled;
        this.files = Preconditions.checkNotNull(files, "files");
        this.truncationAddress = Preconditions.checkNotNull(truncationAddress, "truncationAddress");
        this.updateVersion = new AtomicInteger(updateVersion);
    }

    //endregion

    //region Operations

    /**
     * Creates a new instance of the LogMetadata class which contains an additional file.
     *
     * @param file The path of the file to add.
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata addFile(Path file) {
        Preconditions.checkState(this.enabled, "Log is not enabled. Cannot perform any modifications on it.");

        // Copy existing files.
        List<FileMetadata> newFiles = new ArrayList<>(this.files.size() + 1);
        newFiles.addAll(this.files);

        // Create and add metadata for the new file.
        int sequence = this.files.size() == 0 ? INITIAL_FILE_SEQUENCE : this.files.get(this.files.size() - 1).getSequence() + 1;
        newFiles.add(new FileMetadata(file, sequence));
        return new LogMetadata(this.epoch + 1, this.enabled, Collections.unmodifiableList(newFiles), this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Creates a new instance of the LogMetadata class which contains all the files after (and including) the given address.
     *
     * @param upToAddress The address to truncate to.
     * @return A new instance of the LogMetadata class.
     */
    LogMetadata truncate(FileAddress upToAddress) {
        Preconditions.checkState(this.enabled, "Log is not enabled. Cannot perform any modifications on it.");

        // Exclude all those files that have a sequence less than the one we are given.
        val newFiles = this.files.stream().filter(fm -> fm.getSequence() >= upToAddress.getFileSequence()).collect(Collectors.toList());
        return new LogMetadata(this.epoch, this.enabled, Collections.unmodifiableList(newFiles), upToAddress, this.updateVersion.get());
    }

    /**
     * Removes FileMetadata instances for those files that are known to be empty.
     *
     * @param skipCountFromEnd The number of files to spare, counting from the end of the FileMetadata list.
     * @return A new instance of LogMetadata with the updated file list.
     */
    LogMetadata removeEmptyFiles(int skipCountFromEnd) {
        val newFiles = new ArrayList<FileMetadata>();
        int cutoffIndex = this.files.size() - skipCountFromEnd;
        for (int i = 0; i < cutoffIndex; i++) {
            FileMetadata fm = this.files.get(i);
            if (fm.getStatus() != FileMetadata.Status.Empty) {
                // Not Empty or Unknown: keep it!
                newFiles.add(fm);
            }
        }

        // Add the ones from the end, as instructed.
        for (int i = cutoffIndex; i < this.files.size(); i++) {
            newFiles.add(this.files.get(i));
        }

        return new LogMetadata(this.epoch, this.enabled, Collections.unmodifiableList(newFiles), this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Updates the LastAddConfirmed on individual FileMetadata instances based on the provided argument.
     *
     * @param lastAddConfirmed A Map of File Sequence to LastAddConfirmed based on which we can update the status.
     * @return This (unmodified) instance if lastAddConfirmed.isEmpty() or a new instance of the LogMetadata class with
     * the updated FileMetadata instances.
     */
    LogMetadata updateFileStatus(Map<Path, Long> lastAddConfirmed) {
        if (lastAddConfirmed.isEmpty()) {
            // Nothing to change.
            return this;
        }

        val newFiles = this.files.stream()
                .map(fm -> {
                    long lac = lastAddConfirmed.getOrDefault(fm.getFile(), Long.MIN_VALUE);
                    if (fm.getStatus() == FileMetadata.Status.Unknown && lac != Long.MIN_VALUE) {
                        FileMetadata.Status e = lac == FileLogs.NO_ENTRY_ID
                                ? FileMetadata.Status.Empty
                                : FileMetadata.Status.NotEmpty;
                        fm = new FileMetadata(fm.getFile(), fm.getSequence(), e);
                    }

                    return fm;
                })
                .collect(Collectors.toList());
        return new LogMetadata(this.epoch, this.enabled, Collections.unmodifiableList(newFiles), this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Gets a value indicating the current version of the Metadata (this changes upon every successful metadata persist).
     * Note: this is different from getEpoch() - which gets incremented with every successful recovery.
     *
     * @return The current version.
     */
    @Override
    public int getUpdateVersion() {
        return this.updateVersion.get();
    }

    /**
     * Updates the current version of the metadata.
     *
     * @param value The new metadata version.
     * @return This instance.
     */
    LogMetadata withUpdateVersion(int value) {
        Preconditions.checkArgument(value >= this.updateVersion.get(), "versions must increase");
        this.updateVersion.set(value);
        return this;
    }

    /**
     * Returns a LogMetadata class with the exact contents of this instance, but the enabled flag set to true. No changes
     * are performed on this instance.
     *
     * @return This instance, if isEnabled() == true, of a new instance of the LogMetadata class which will have
     * isEnabled() == true, otherwise.
     */
    LogMetadata asEnabled() {
        return this.enabled ? this : new LogMetadata(this.epoch, true, this.files, this.truncationAddress, this.updateVersion.get());
    }

    /**
     * Returns a LogMetadata class with the exact contents of this instance, but the enabled flag set to false. No changes
     * are performed on this instance.
     *
     * @return This instance, if isEnabled() == false, of a new instance of the LogMetadata class which will have
     * isEnabled() == false, otherwise.
     */
    LogMetadata asDisabled() {
        return this.enabled ? new LogMetadata(this.epoch, false, this.files, this.truncationAddress, this.updateVersion.get()) : this;
    }

    /**
     * Gets the FileMetadata for the file with given path.
     *
     * @param file The file path to search.
     * @return The sought FileMetadata, or null if not found.
     */
    FileMetadata getFile(Path file) {
       return getFileMetadataIndex(file);
    }

    /**
     * Gets the File Address immediately following the given address.
     *
     * @param address     The current address.
     * @param lastEntryId If known, then Entry Id of the last entry in the file to which address is pointing. This is
     *                    used to determine if the next address should be returned on the next file. If not known,
     *                    this should be Long.MAX_VALUE, in which case the next address will always be on the same file.
     * @return The next address, or null if no such address exists (i.e., if we reached the end of the log).
     */
    FileAddress getNextAddress(FileAddress address, long lastEntryId) {
        if (this.files.size() == 0) {
            // Quick bail-out. Nothing to return.
            return null;
        }

        FileAddress result = null;
        FileMetadata firstFile = this.files.get(0);
        if (address.getFileSequence() < firstFile.getSequence()) {
            // Most likely an old address. The result is the first address of the first file we have.
            result = new FileAddress(firstFile, 0);
        } else if (address.getEntryId() < lastEntryId) {
            // Same file, next entry.
            result = new FileAddress(address.getFileSequence(), address.getFile(), address.getEntryId() + 1);
        } else {
            // Next file. First try a binary search, hoping the file in the address actually exists.
            FileMetadata fileMetadata = null;
            int index = getFileMetadataIndex(address.getFileSequence()) + 1;
            if (index > 0) {
                // File is in the list. Make sure it's not the last one.
                if (index < this.files.size()) {
                    fileMetadata = this.files.get(index);
                }
            } else {
                // File was not in the list. We need to find the first file with a sequence larger than the one we have.
                for (FileMetadata fm : this.files) {
                    if (fm.getSequence() > address.getFileSequence()) {
                        fileMetadata = fm;
                        break;
                    }
                }
            }

            if (fileMetadata != null) {
                result = new FileAddress(fileMetadata, 0);
            }
        }

        if (result != null && result.compareTo(this.truncationAddress) < 0) {
            result = this.truncationAddress;
        }

        return result;
    }

    private int getFileMetadataIndex(int fileSequence) {
        return CollectionHelpers.binarySearch(this.files, fm -> Integer.compare(fileSequence, fm.getSequence()));
    }

    private FileMetadata getFileMetadataIndex(Path file) {
        return this.files.stream().filter(fm -> fm.getFile().compareTo(file) == 0).findFirst().orElse(null);
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Version = %d, Epoch = %d, FileCount = %d, Truncate = (%s-%d)",
                this.updateVersion.get(), this.epoch, this.files.size(), this.truncationAddress.getFile(), this.truncationAddress.getEntryId());
    }

    //region Serialization

    static class LogMetadataBuilder implements ObjectBuilder<LogMetadata> {
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<LogMetadata, LogMetadataBuilder> {
        @Override
        protected LogMetadataBuilder newBuilder() {
            return LogMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(LogMetadata m, RevisionDataOutput output) throws IOException {
            output.writeBoolean(m.isEnabled());
            output.writeCompactLong(m.getEpoch());
            output.writeCompactLong(m.truncationAddress.getSequence());
            output.writeUTF(m.truncationAddress.getFile().toString());
            output.writeCollection(m.files, this::writeFile00);
        }

        private void read00(RevisionDataInput input, LogMetadataBuilder builder) throws IOException {
            builder.enabled(input.readBoolean());
            builder.epoch(input.readCompactLong());
            builder.truncationAddress(new FileAddress(input.readCompactLong(), Paths.get(input.readUTF())));
            List<FileMetadata> files = input.readCollection(this::readFile00, ArrayList::new);
            builder.files(Collections.unmodifiableList(files));
            builder.updateVersion(INITIAL_VERSION);
        }

        private void writeFile00(RevisionDataOutput output, FileMetadata m) throws IOException {
            output.writeUTF(m.getFile().toString());
            output.writeCompactInt(m.getSequence());
            output.writeByte(m.getStatus().getValue());
        }

        private FileMetadata readFile00(RevisionDataInput input) throws IOException {
            Path file = Paths.get(input.readUTF());
            int seq = input.readCompactInt();
            FileMetadata.Status empty = FileMetadata.Status.valueOf(input.readByte());
            return new FileMetadata(file, seq, empty);
        }
    }

    //endregion
}
