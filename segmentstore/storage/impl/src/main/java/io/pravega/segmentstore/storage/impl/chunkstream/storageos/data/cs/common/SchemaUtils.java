package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class SchemaUtils {

    public static String makeListToken(int ctIndex, UUID startChunkId) throws IOException {
        var ctToken = SchemaKeyRecords.SchemaKey.newBuilder()
                                                .setType(SchemaKeyRecords.SchemaKeyType.CHUNK)
                                                .setUserKey(SchemaKeyRecords.ChunkKey.newBuilder()
                                                                                     .setChunkIdUuid(SchemaKeyRecords.Uuid.newBuilder()
                                                                                                                          .setHighBytes(startChunkId.getMostSignificantBits())
                                                                                                                          .setLowBytes(startChunkId.getLeastSignificantBits())
                                                                                                                          .build())
                                                                                     .build().toByteString())
                                                .build();
        return makeListToken(ctIndex, ctToken);
    }

    public static String makeListToken(int ctIndex, SchemaKeyRecords.SchemaKey ctToken) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ctToken.getSerializedSize() + 4);
        buffer.putInt(ctIndex);
        ctToken.writeTo(CodedOutputStream.newInstance(buffer.array(), buffer.position(), buffer.remaining()));
        return Base64.encodeBase64String(buffer.array());
    }

    public static Pair<Integer, SchemaKeyRecords.SchemaKey> parseListToken(String token) throws IOException {
        var buf = ByteBuffer.wrap(Base64.decodeBase64(token));
        var ctIndex = buf.getInt();
        var key = SchemaKeyRecords.SchemaKey.parseFrom(CodedInputStream.newInstance(buf.array(), buf.position(), buf.remaining()));
        return new ImmutablePair<>(ctIndex, key);
    }

    public static String getDeviceId(CmMessage.SSLocation location) {
        return location.hasSsId() ? location.getSsId() : new UUID(location.getSsIdUuid().getHighBytes(), location.getSsIdUuid().getLowBytes()).toString();
    }

    public static String getPartitionId(CmMessage.SSLocation location) {
        return location.hasPartitionId() ? location.getPartitionId() : new UUID(location.getPartitionIdUuid().getHighBytes(), location.getPartitionIdUuid().getLowBytes()).toString();
    }
}
