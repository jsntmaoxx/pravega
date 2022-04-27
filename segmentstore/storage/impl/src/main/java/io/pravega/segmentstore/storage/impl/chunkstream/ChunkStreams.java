package io.pravega.segmentstore.storage.impl.chunkstream;

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.ChunkStream;

/**
 * General utilities pertaining to Chunk Streams.
 */
public class ChunkStreams {

    /**
     * For special log repair operations, we allow the LogReader to read from a special log id that contains
     * admin-provided changes to repair the original lod data.
     */
    static final int REPAIR_LOG_ID = Integer.MAX_VALUE;

    /**
     * For special log repair operations, we need to store the original content of the damaged log on a temporary backup
     * log.
     */
    static final int BACKUP_LOG_ID = Integer.MAX_VALUE - 1;

    /**
     * Open a Chunk Stream.
     * @param streamId The id of the stream to open.
     * @param stream A {@link ChunkStream} to open.
     * @throws DurableDataLogException If another exception occurred. The causing exception is wrapped inside it.
     */
    static void open(String streamId, ChunkStream stream) throws DurableDataLogException {
        try {
            stream.open(streamId, true).get();
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to open chunk stream %s.", streamId), ex);
        }
    }

    /**
     * Closes the given Chunk Stream.
     *
     * @param stream A {@link ChunkStream} to close.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void close(ChunkStream stream) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(stream::close);
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to close Chunk Stream %s.", stream.getStreamId()), ex);
        }
    }

    /**
     * Truncate the Chunk Stream to given address.
     *
     * @param stream The ChunkStream to truncate.
     * @param address The address up to which to truncate.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void truncate(ChunkStream stream, StreamAddress address) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(() -> stream.truncate(address.getSequence()));
        } catch (Exception ex) {
            throw new DurableDataLogException(String.format("Unable to truncate Chunk Stream %s up to %s.", stream.getStreamId(), address), ex);
        }
    }
}
