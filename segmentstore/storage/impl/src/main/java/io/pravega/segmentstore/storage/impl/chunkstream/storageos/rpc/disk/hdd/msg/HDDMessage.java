package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.hdd.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;

public interface HDDMessage extends DiskMessage {
    int VERSION = 1;
    ByteString DEFAULT_CHECKSUM = ByteString.copyFrom(new byte[]{'-', '1'});

    int waitDataBytes();

    void feedData(ByteBuf in);
}
