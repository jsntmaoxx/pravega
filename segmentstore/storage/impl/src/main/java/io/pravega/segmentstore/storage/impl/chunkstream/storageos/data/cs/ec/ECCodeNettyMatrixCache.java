package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ECCodeNettyMatrixCache extends ECBufferCache<Long> {
    private static final Logger log = LoggerFactory.getLogger(ECCodeNettyMatrixCache.class);
    private final Map<Long, ByteBuf> byteBufMap = new ConcurrentHashMap<>();

    protected ECCodeNettyMatrixCache(ECSchema ecSchema, int initFreeCount, int maxCreateCount) {
        super(ecSchema, maxCreateCount);
        feedBuffers(initFreeCount);
    }

    @Override
    public Long segmentOfBuffer(Long buffer, int segIndex) {
        return G.U.getLong(buffer + (long) segIndex * Long.BYTES);
    }

    @Override
    protected Long createBuffer() {
        return makeByteBufMatrix(ecSchema.CodeNumber, ecSchema.SegmentLength, (byte) 0);
    }

    @Override
    protected void clearBuffer(Long buffer) {
        //setByteBufMatrix(buffer, ecSchema.CodeNumber, ecSchema.SegmentLength, (byte) 0);
    }

    @Override
    protected void destroyBuffer(Long buffer) {
        destroyByteBufMatrix(buffer, ecSchema.CodeNumber);
    }

    @Override
    public List<ByteBuf> toByteBufList(long buffer) throws CSException {
        var bufList = new ArrayList<ByteBuf>(ecSchema.CodeNumber);
        for (int i = 0; i < ecSchema.CodeNumber; ++i) {
            var address = G.U.getLong(buffer + (long) i * Long.BYTES);
            var buf = byteBufMap.get(address);
            if (buf == null) {
                log().error("WSCritical: did not find code matrix {} seg {} ByteBuf {}",
                            buffer, i, address);
                throw new CSException("matrix " + Long.toHexString(buffer) + " has no ByteBuf of seg " + i);
            }
            bufList.add(buf.duplicate().writerIndex(ecSchema.SegmentLength));
        }
        return bufList;
    }

    @Override
    protected Logger log() {
        return log;
    }

    private long makeByteBufMatrix(int dataNum, int segLen, byte v) {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        for (int i = 0; i < dataNum; ++i) {
            var buf = Unpooled.directBuffer(segLen, segLen);
            var address = buf.memoryAddress();
            G.U.setMemory(address, segLen, v);
            var old = byteBufMap.put(address, buf);
            if (old != null) {
                log().error("WSCritical: code matrix {} seg {} ByteBuf {} already exist",
                            matrix, i, address);
            }
            G.U.putAddress(matrix + (long) i * Long.BYTES, address);
        }
        return matrix;
    }

    private void destroyByteBufMatrix(long matrix, int dataNum) {
        for (int i = 0; i < dataNum; ++i) {
            var address = G.U.getLong(matrix + (long) i * Long.BYTES);
            var byteBuf = byteBufMap.remove(address);
            if (byteBuf == null) {
                log().error("WSCritical: did not find code matrix {} seg {} ByteBuf {}",
                            matrix, i, address);
            } else {
                byteBuf.release();
            }
        }
        EC.destroy_buffer(matrix);
    }

    private void setByteBufMatrix(long matrix, int dataNum, int segLen, byte v) {
        for (int i = 0; i < dataNum; ++i) {
            var address = G.U.getLong(matrix + (long) i * Long.BYTES);
            G.U.setMemory(address, segLen, v);
        }
    }
}
