package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.stream.write;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.SizeUtils;

/*
StreamDataCacheQueue is not thread safe
 */
public class StreamDataCacheQueue {
    private final StreamWriteDataBuffer[] vData;
    private final long modMask;
    private final String steamId;
    private long start = 0;
    private long end = 0;

    public StreamDataCacheQueue(String steamId, int capacity) {
        this.steamId = steamId;
        var cap = SizeUtils.aroundToPowerOf2(capacity);
        this.modMask = SizeUtils.modMask(cap);
        vData = new StreamWriteDataBuffer[cap];
    }

    public void push(StreamWriteDataBuffer data) throws CSException {
        if (end - start < vData.length) {
            vData[(int) ((end++) & modMask)] = data;
            return;
        }
        throw new CSException("stream " + steamId + " data cache queue is full at capacity " + (end - start));
    }

    public StreamWriteDataBuffer peak() {
        if (start < end) {
            return vData[(int)(start & modMask)];
        }
        return null;
    }

    public boolean pop() {
        if (start < end) {
            ++start;
            return true;
        }
        return false;
    }
}
