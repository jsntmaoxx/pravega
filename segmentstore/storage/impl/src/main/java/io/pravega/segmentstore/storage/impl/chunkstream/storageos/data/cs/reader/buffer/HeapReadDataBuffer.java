package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class HeapReadDataBuffer extends JDKReadDataBuffer {
    private static final Logger log = LoggerFactory.getLogger(HeapReadDataBuffer.class);

    public HeapReadDataBuffer(int capacity) {
        super(ByteBuffer.allocate(capacity));
    }

    @Override
    protected Logger log() {
        return log;
    }

    @Override
    protected long computeCRC32(ByteBuffer inputData, int startPos, int size) {
        CRC32 crc32 = new CRC32();
        crc32.update(inputData.array(), inputData.arrayOffset() + startPos, size);
        return crc32.getValue();
    }

    @Override
    protected long computeCRC32(ByteBuffer inputData1, int startPos1, int size1, ByteBuffer inputData2, int startPos2, int size2) {
        CRC32 crc32 = new CRC32();
        crc32.update(inputData1.array(), inputData1.arrayOffset() + startPos1, size1);
        crc32.update(inputData2.array(), inputData2.arrayOffset() + startPos2, size2);
        return crc32.getValue();
    }

    @Override
    public void put(ByteBuf in) {
        var len = Math.min(in.readableBytes(), data.remaining());
        in.readBytes(data.array(), data.arrayOffset() + data.position(), len);
        data.position(data.position() + len);
    }

    @Override
    public void put(ByteBuffer buffer) {
        var len = Math.min(buffer.remaining(), data.remaining());
        buffer.get(data.array(), data.arrayOffset() + data.position(), len);
        data.position(data.position() + len);
    }

    @Override
    public long nativeDataAddress() {
        throw new UnsupportedOperationException("HeapReadDataBuffer not support nativeDataAddress");
    }
}
