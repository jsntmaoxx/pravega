package io.pravega.segmentstore.storage.impl.chunkstream;

import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for Chunk Stream Client*/
public class ChunkStreamConfig {
    //region Config Names

    public static final Property<Integer> CHUNK_STREAM_WRITE_TIMEOUT = Property.named("write.timeout.milliseconds", 60000, "chunkStreamWriteTimeoutMillis");
    public static final Property<Integer> MAX_OUTSTANDING_BYTES = Property.named("write.outstanding.bytes.max", 256 * 1024 * 1024, "maxOutstandingBytes");

    /**
     * Maximum append length, as specified by Chunk Stream Client.
     */
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;

    //endregion

    //region Members

    /**
     * The Write Timeout (Chunk Stream Client), in milliseconds.
     */
    @Getter
    private final int chunkStreamWriteTimeoutMillis;

    /**
     * The maximum number of bytes that can be outstanding per ChunkStreamLog at any given time. This value should be used
     * for throttling purposes.
     */
    @Getter
    private final int maxOutstandingBytes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ChunkStreamConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ChunkStreamConfig(TypedProperties properties) throws ConfigurationException {
        this.chunkStreamWriteTimeoutMillis = properties.getInt(CHUNK_STREAM_WRITE_TIMEOUT);
        this.maxOutstandingBytes = properties.getInt(MAX_OUTSTANDING_BYTES);
    }
}
