package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class DirectReadDataBuffer extends JDKReadDataBuffer {
    private static final Logger log = LoggerFactory.getLogger(DirectReadDataBuffer.class);

    public DirectReadDataBuffer(int capacity) {
        super(ByteBuffer.allocateDirect(capacity));
    }

    public DirectReadDataBuffer(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected long computeCRC32(ByteBuffer inputData, int startPos, int size) {
        var d = (DirectBuffer) inputData;
        return CRC.crc32(0, d.address() + startPos, size);
    }

    @Override
    protected long computeCRC32(ByteBuffer inputData1, int startPos1, int size1, ByteBuffer inputData2, int startPos2, int size2) {
        var d1 = (DirectBuffer) inputData1;
        var crc1 = CRC.crc32(0, d1.address() + startPos1, size1);
        var d2 = (DirectBuffer) inputData2;
        return CRC.crc32(crc1, d2.address() + startPos2, size2);
    }

    @Override
    public void put(ByteBuf in) {
        var len = Math.min(in.readableBytes(), data.remaining());
        var oldLimit = data.limit();
        data.limit(data.position() + len);
        in.readBytes(data);
        data.limit(oldLimit);
    }

    @Override
    public void put(ByteBuffer buffer) {
        if (data.remaining() >= buffer.remaining()) {
            data.put(buffer);
        } else {
            data.put(buffer.duplicate().limit(buffer.position() + data.remaining()));
        }
    }

    @Override
    public long nativeDataAddress() {
        return ((DirectBuffer) data).address();
    }
}
