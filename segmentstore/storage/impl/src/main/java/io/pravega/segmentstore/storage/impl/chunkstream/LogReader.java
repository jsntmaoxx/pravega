package io.pravega.segmentstore.storage.impl.chunkstream;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.ChunkStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Performs read from ChunkStream Logs.
 */
@Slf4j
@NotThreadSafe
public class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {

    //region Members

    private final String logId;
    private final ChunkStream logStream;
    private final LogMetadata metadata;
    private final AtomicBoolean closed;
    private StreamAddress currentAddress;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogReader class.
     *
     * @param logId      The Id of the {@link ChunkStreamLog} to read from. This is used for validation purposes.
     * @param metadata   The LogMetadata of the Log to read.
     * @param logStream  The ChunkStream of the Log to read.
     */
    LogReader(String logId, LogMetadata metadata, ChunkStream logStream) {
        this.logId = logId;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.logStream = Preconditions.checkNotNull(logStream, "logStream");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.currentAddress = null;
        }
    }

    //endregion

    //region CloseableIterator Implementation
    @Override
    public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Pair<Long, ByteBuffer> readEntry;
        try {
            if (this.currentAddress == null) {
                readEntry = this.logStream.read(this.metadata.getTruncationAddress().getSequence()).get();
            } else {
                readEntry = this.logStream.read(this.currentAddress.getSequence()).get();
            }
        } catch (Exception ex) {
            close();
            throw new DurableDataLogException("Error while reading from chunk stream.", ex);
        }
        byte[] data = new byte[readEntry.getRight().remaining()];
        readEntry.getRight().get(data);
        long offset = readEntry.getLeft();
        this.currentAddress = new StreamAddress(offset + data.length);

        return wrapItem(data, offset);
    }

    //endregion

    //region ReadItem

    private static class ReadItem implements DurableDataLog.ReadItem {
        @Getter
        private final InputStream payload;
        @Getter
        private final int length;
        @Getter
        private final StreamAddress address;

        ReadItem(InputStream payload, int length, long offset) {
            this.payload = payload;
            this.length = length;
            this.address = new StreamAddress(offset);
        }

        @Override
        public String toString() {
            return String.format("%s, Length = %d.", this.address, this.length);
        }
    }

    private static DurableDataLog.ReadItem wrapItem(byte[] data, long offset) {
        return new ReadItem(new ByteArrayInputStream(data), data.length, offset);
    }

    //endregion
}
