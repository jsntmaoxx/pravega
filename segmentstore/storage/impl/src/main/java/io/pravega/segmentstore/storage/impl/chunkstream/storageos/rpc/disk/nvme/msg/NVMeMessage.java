package io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.nvme.msg;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import io.netty.buffer.ByteBuf;

public abstract class NVMeMessage implements DiskMessage {

    public static final int MSG_TYPE_LEN = 1;
    public static final int MSG_REQUEST_ID_LEN = 32;
    public static final int MSG_DATA_SIZE_LEN = 4;
    public static final int MSG_SEGMENT_INFO_LEN = 48;
    public static final int MSG_SEGMENT_INFO_OFFSET_LEN = 8;
    public static final int MSG_SEGMENT_INFO_PARTITION_UUID_LEN = 36;
    public static final int MSG_SEGMENT_INFO_NODE_IP_LEN = 4;
    public static final int MSG_MAGIC_LEN = 4;
    public static final int MSG_TIMESTAMP_LEN = 8;
    public static final int MSG_STATUS_CODE_LEN = 1;
    public static final int MSG_OFFSET_IN_DATA_LEN = 4;
    public static final int MSG_SHM_BLOCK_ID_LEN = 4;

    /*
    NVMe request header format:
     |  type  | requestId | dataSize |        segmentInfo0        | segmentInfo1 | segmentInfo2 | magic  | time   |
                                     | offset0 |  uuid0  |   ip0  |
     |        |  String   |   int    |   long  |  String |   int  |              |              |  int   | long   |
     |  1byte |  32bytes  |  4bytes  |  8bytes | 36bytes | 4bytes |    48bytes   |    48bytes   | 4bytes | 8bytes |
     */
    public static final int REQUEST_TYPE_OFFSET = 0;
    public static final int REQUEST_REQUEST_ID_OFFSET = REQUEST_TYPE_OFFSET + MSG_TYPE_LEN;
    public static final int REQUEST_DATA_SIZE_OFFSET = REQUEST_REQUEST_ID_OFFSET + MSG_REQUEST_ID_LEN;
    // seg0
    public static final int REQUEST_SEGMENT_INFO_0_OFFSET = REQUEST_DATA_SIZE_OFFSET + MSG_DATA_SIZE_LEN;
    public static final int REQUEST_SEGMENT_INFO_0_OFFSET_OFFSET = REQUEST_SEGMENT_INFO_0_OFFSET;
    public static final int REQUEST_SEGMENT_INFO_0_PARTITION_UUID_OFFSET = REQUEST_SEGMENT_INFO_0_OFFSET_OFFSET + MSG_SEGMENT_INFO_OFFSET_LEN;
    public static final int REQUEST_SEGMENT_INFO_0_NODE_IP_OFFSET = REQUEST_SEGMENT_INFO_0_PARTITION_UUID_OFFSET + MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
    // seg1
    public static final int REQUEST_SEGMENT_INFO_1_OFFSET = REQUEST_SEGMENT_INFO_0_OFFSET + MSG_SEGMENT_INFO_LEN;
    public static final int REQUEST_SEGMENT_INFO_1_OFFSET_OFFSET = REQUEST_SEGMENT_INFO_1_OFFSET;
    public static final int REQUEST_SEGMENT_INFO_1_PARTITION_UUID_OFFSET = REQUEST_SEGMENT_INFO_1_OFFSET_OFFSET + MSG_SEGMENT_INFO_OFFSET_LEN;
    public static final int REQUEST_SEGMENT_INFO_1_NODE_IP_OFFSET = REQUEST_SEGMENT_INFO_1_PARTITION_UUID_OFFSET + MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
    // seg2
    public static final int REQUEST_SEGMENT_INFO_2_OFFSET = REQUEST_SEGMENT_INFO_1_OFFSET + MSG_SEGMENT_INFO_LEN;
    public static final int REQUEST_SEGMENT_INFO_2_OFFSET_OFFSET = REQUEST_SEGMENT_INFO_2_OFFSET;
    public static final int REQUEST_SEGMENT_INFO_2_PARTITION_UUID_OFFSET = REQUEST_SEGMENT_INFO_2_OFFSET_OFFSET + MSG_SEGMENT_INFO_OFFSET_LEN;
    public static final int REQUEST_SEGMENT_INFO_2_NODE_IP_OFFSET = REQUEST_SEGMENT_INFO_2_PARTITION_UUID_OFFSET + MSG_SEGMENT_INFO_PARTITION_UUID_LEN;
    //
    public static final int REQUEST_MAGIC_OFFSET = REQUEST_SEGMENT_INFO_2_OFFSET + MSG_SEGMENT_INFO_LEN;
    public static final int REQUEST_TIMESTAMP_OFFSET = REQUEST_MAGIC_OFFSET + MSG_MAGIC_LEN;
    // total 193
    public static final int REQUEST_HEADER_LENGTH = REQUEST_TIMESTAMP_OFFSET + MSG_TIMESTAMP_LEN;

    /*
    NVMe response header format:
    |  type  | requestId | dataSize |      statusCode         | MAGIC | time_ns | offset in data | shm block id |
    |        |  String   |   int    |status | cp0 | cp1 | cp2 |  int  |  long   |                |   int        |
    |  1byte |  32bytes  |  4bytes  | byte  |byte |byte |byte | 4bytes|  8bytes |     4bytes     |   4bytes     |
    */
    public static final int RESPONSE_TYPE_OFFSET = 0;
    public static final int RESPONSE_REQUEST_ID_OFFSET = RESPONSE_TYPE_OFFSET + MSG_TYPE_LEN;
    public static final int RESPONSE_DATA_SIZE_OFFSET = RESPONSE_REQUEST_ID_OFFSET + MSG_REQUEST_ID_LEN;
    // status
    public static final int RESPONSE_STATUS_CODE_OFFSET = RESPONSE_DATA_SIZE_OFFSET + MSG_DATA_SIZE_LEN;
    public static final int RESPONSE_STATUS_CODE_CP0_OFFSET = RESPONSE_STATUS_CODE_OFFSET + MSG_STATUS_CODE_LEN;
    public static final int RESPONSE_STATUS_CODE_CP1_OFFSET = RESPONSE_STATUS_CODE_CP0_OFFSET + MSG_STATUS_CODE_LEN;
    public static final int RESPONSE_STATUS_CODE_CP2_OFFSET = RESPONSE_STATUS_CODE_CP1_OFFSET + MSG_STATUS_CODE_LEN;
    //
    public static final int RESPONSE_MAGIC_OFFSET = RESPONSE_STATUS_CODE_CP2_OFFSET + MSG_STATUS_CODE_LEN;
    public static final int RESPONSE_TIMESTAMP_OFFSET = RESPONSE_MAGIC_OFFSET + MSG_MAGIC_LEN;
    // shm
    public static final int RESPONSE_OFFSET_IN_DATA_OFFSET = RESPONSE_TIMESTAMP_OFFSET + MSG_TIMESTAMP_LEN;
    public static final int RESPONSE_SHM_BLOCK_ID_OFFSET = RESPONSE_OFFSET_IN_DATA_OFFSET + MSG_OFFSET_IN_DATA_LEN;
    // total 61
    public static final int RESPONSE_HEADER_LENGTH = RESPONSE_SHM_BLOCK_ID_OFFSET + MSG_SHM_BLOCK_ID_LEN;
    public static final int MSG_MAGIC_VALUE = 0x4E564D46;

    public abstract int waitDataBytes();

    public abstract void feedData(ByteBuf in);

    public enum MessageType {
        READ,
        WRITE_ONE_COPY,
        WRITE_THREE_COPIES,
        TRIM,
        PING,
        HUGE_SHM_INFO_RESPONSE,
        HUGE_SHM_RESPONSE,
        HUGE_SHM_INFO_REQUEST,
        UNKNOWN;

        public static MessageType from(byte value) throws CSException {
            for (var e : values()) {
                if (e.ordinal() == value) {
                    return e;
                }
            }
            throw new CSException("unknown MSG_TYPE for value " + value);
        }
    }
}
