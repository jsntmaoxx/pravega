package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.Cluster;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTLevel;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.FileOperationsPayload;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;

import static io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.FileOperationsPayloads.CommandType.RESPONSE_PING;

public class DTMessageBuilder {
    public static final int HeaderLength = Long.BYTES + Integer.BYTES; // timestamp + size
    public static final int FooterLength = Long.BYTES; // checksum

    public static FileOperationsPayload makePingResponse(FileOperationsPayload request) {
        return FileOperationsPayload.newBuilder()
                                    .setCommandType(RESPONSE_PING)
                                    .setRequestId(request.getRequestId())
                                    .setCos(request.getCos())
                                    .setType(request.getType())
                                    .setKeyHash(0)
                                    .setLevel(0)
                                    .setPayload(ByteString.EMPTY)
                                    .build();
    }

    public static FileOperationsPayload makeFileOperationPayloads(Cluster cluster,
                                                                  String requestId,
                                                                  FileOperationsPayloads.CommandType commandType,
                                                                  DTType dtType,
                                                                  DTLevel dtLevel,
                                                                  int dtHash,
                                                                  GeneratedMessage request) {
        return FileOperationsPayload.newBuilder()
                                    .setRequestId(requestId)
                                    .setCommandType(commandType)
                                    .setCos(cluster.currentCos())
                                    .setType(dtType.toString())
                                    .setLevel(dtLevel.intLevel())
                                    .setKeyHash(dtHash)
                                    .setPayload(request.toByteString())
                                    .setResponsePort(Cluster.csInternalPort)
                                    .setCallerId(cluster.currentDataIp())
                                    .build();
    }

    public static FileOperationsPayload makeFileOperationPayloads(String requestId,
                                                                  FileOperationsPayloads.CommandType commandType,
                                                                  DTType dtType,
                                                                  DTLevel dtLevel,
                                                                  int dtHash,
                                                                  String cos,
                                                                  String dataIp,
                                                                  GeneratedMessage request) {

        return FileOperationsPayload.newBuilder()
                                    .setCommandType(commandType)
                                    .setRequestId(requestId)
                                    .setCos(cos)
                                    .setType(dtType.toString())
                                    .setKeyHash(dtHash)
                                    .setLevel(dtLevel.intLevel())
                                    .setPayload(request.toByteString())
                                    .setResponsePort(Cluster.csInternalPort)
                                    .setCallerId(dataIp).build();
    }

    public static ByteBuf makeByteBuf(FileOperationsPayload msg, long createNano) throws IOException {
        var msgLength = msg.getSerializedSize();
        var buf = Unpooled.directBuffer(HeaderLength + msgLength + FooterLength);
        buf.writeLong(createNano);
        buf.writeInt(msgLength + FooterLength);
        msg.writeTo(new ByteBufOutputStream(buf));
        var crcValue = CRC.crc32(0, buf.memoryAddress(), HeaderLength + msgLength);
        buf.writeLong(crcValue);
        return buf;
    }
}
