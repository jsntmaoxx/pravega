package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.ECUtils;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ECCodeNativeMatrixCache extends ECBufferCache<Long> {
    private static final Logger log = LoggerFactory.getLogger(ECCodeNativeMatrixCache.class);

    protected ECCodeNativeMatrixCache(ECSchema ecSchema, int initFreeCount, int maxCreateCount) {
        super(ecSchema, maxCreateCount);
        feedBuffers(initFreeCount);
    }

    @Override
    public Long segmentOfBuffer(Long buffer, int segIndex) {
        return G.U.getLong(buffer + (long) segIndex * Long.BYTES);
    }

    @Override
    protected Long createBuffer() {
        return ECUtils.makeMatrix(ecSchema.CodeNumber, ecSchema.SegmentLength, (byte) 0);
    }

    @Override
    protected void clearBuffer(Long buffer) {
        //ECUtils.setMatrix(buffer, ecSchema.CodeNumber, ecSchema.SegmentLength, (byte) 0);
    }

    @Override
    protected void destroyBuffer(Long buffer) {
        ECUtils.destroyMatrix(buffer, ecSchema.CodeNumber);
    }

    @Override
    public List<ByteBuf> toByteBufList(long codeBufferAddress) {
        throw new UnsupportedOperationException("ECCodeNativeMatrixCache is not support toByteBufList");
    }

    @Override
    protected Logger log() {
        return log;
    }
}
