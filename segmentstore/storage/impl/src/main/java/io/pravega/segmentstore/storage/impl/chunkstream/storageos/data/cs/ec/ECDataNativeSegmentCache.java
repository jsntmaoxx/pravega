package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ECDataNativeSegmentCache extends ECBufferCache<Long> {
    private static final Logger log = LoggerFactory.getLogger(ECDataNativeSegmentCache.class);

    protected ECDataNativeSegmentCache(ECSchema ecSchema, int initFreeCount, int maxCreateCount) {
        super(ecSchema, maxCreateCount);
        feedBuffers(initFreeCount);
    }

    @Override
    public Long segmentOfBuffer(Long buffer, int segIndex) {
        return buffer;
    }

    @Override
    protected Long createBuffer() {
        return EC.make_buffer(ecSchema.SegmentLength, 1);
    }

    @Override
    protected void clearBuffer(Long buffer) {
        //G.U.setMemory(buffer, ecSchema.SegmentLength, (byte) 0);
    }

    @Override
    protected void destroyBuffer(Long buffer) {
        EC.destroy_buffer(buffer);
    }

    @Override
    public List<ByteBuf> toByteBufList(long codeBufferAddress) {
        throw new UnsupportedOperationException("ECDataNativeSegmentCache is not support toByteBufList");
    }

    @Override
    protected Logger log() {
        return log;
    }
}
