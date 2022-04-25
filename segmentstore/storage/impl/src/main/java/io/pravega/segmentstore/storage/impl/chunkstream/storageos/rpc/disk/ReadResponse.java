package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;

public interface ReadResponse extends DiskMessage {
    ReadDataBuffer data();
}
