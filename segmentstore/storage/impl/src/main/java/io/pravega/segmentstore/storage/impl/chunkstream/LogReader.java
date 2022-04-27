package io.pravega.segmentstore.storage.impl.chunkstream;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.Ledgers;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ChunkConfig;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.ChunkStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Performs read from ChunkStream Logs.
 */
@Slf4j
@NotThreadSafe
public class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {

    //region Members

    private final String logId;
    private final CmClient cmClient;
    private final LogMetadata metadata;
    private final AtomicBoolean closed;
    private final ChunkConfig chunkConfig;
    private final ChunkStreamConfig config;
    private final Executor executor;
    private ChunkStream logStream;
    private StreamAddress currentAddress;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogReader class.
     *
     * @param logId      The id of the {@link ChunkStreamLog} to read from. This is used for validation purposes.
     * @param metadata   The LogMetadata of the Log to read.
     * @param cmClient   A reference to the cm client to use.
     * @param config     Configuration to use.
     */
    LogReader(String logId, LogMetadata metadata, CmClient cmClient, ChunkConfig chunkConfig, ChunkStreamConfig config, Executor executor) {
        this.logId = logId;
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.cmClient = Preconditions.checkNotNull(cmClient, "cmClient");
        this.chunkConfig = Preconditions.checkNotNull(chunkConfig, "chunkConfig");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.logStream != null) {
                try {
                    ChunkStreams.close(this.logStream);
                } catch (DurableDataLogException ex) {
                    log.error("Unable to close chunk stream {}.", this.logStream.getStreamId(), ex);
                }
                this.logStream = null;
            }
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
            if (this.logStream == null) {
                ChunkStream logStream = new ChunkStream(cmClient, this.chunkConfig, executor);
                ChunkStreams.open(this.logId, logStream);
                this.logStream = logStream;
            }
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
