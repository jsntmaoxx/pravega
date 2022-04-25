package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ECBufferCache<T> {
    protected final ECSchema ecSchema;
    private final int maxCreateCount;
    private final AtomicInteger createCount = new AtomicInteger(0);
    private final AtomicInteger borrowedCount = new AtomicInteger(0);
    private final AtomicInteger returnCount = new AtomicInteger(0);
    private final AtomicInteger freeCount = new AtomicInteger(0);
    private final Queue<T> freeBuffers = new ConcurrentLinkedQueue<>();

    protected ECBufferCache(ECSchema ecSchema, int maxCreateCount) {
        this.ecSchema = ecSchema;
        this.maxCreateCount = maxCreateCount;
    }

    public T borrow() {
        var b = freeBuffers.poll();
        if (b == null) {
            if (createCount.getOpaque() >= maxCreateCount) {
                log().error("can not create buffer since have reach max create count {}," +
                            " current create {} borrowed {} returned {} free {}",
                            maxCreateCount, createCount.getOpaque(), borrowedCount.getOpaque(),
                            returnCount.getOpaque(), freeCount.getOpaque());
                return null;
            }
            b = createBuffer();
            if (b == null) {
                log().error("create buffer failed");
                return null;
            }
            createCount.incrementAndGet();
        } else {
            freeCount.decrementAndGet();
        }
        borrowedCount.incrementAndGet();
        return b;
    }

    public void giveBack(T buffer) {
        if (buffer == null) {
            return;
        }
        clearBuffer(buffer);
        freeBuffers.add(buffer);
        freeCount.incrementAndGet();
        returnCount.incrementAndGet();
    }

    public int createCount() {
        return createCount.getOpaque();
    }

    public int borrowedCount() {
        return borrowedCount.getOpaque();
    }

    public int freeCount() {
        return freeCount.getOpaque();
    }

    public abstract T segmentOfBuffer(T buffer, int segIndex);

    protected abstract T createBuffer();

    protected abstract void clearBuffer(T buffer);

    // cj_todo add task release part buffer after some time
    protected abstract void destroyBuffer(T buffer);

    public abstract List<ByteBuf> toByteBufList(long codeBufferAddress) throws CSException;

    @Override
    public String toString() {
        return String.format("create %s borrowed %s returned %s free %s",
                             createCount.getOpaque(), borrowedCount.getOpaque(), returnCount.getOpaque(), freeCount.getOpaque());
    }

    protected abstract Logger log();

    protected void feedBuffers(int count) {
        for (var i = 0; i < count; ++i) {
            var b = createBuffer();
            if (b != null) {
                freeCount.incrementAndGet();
                createCount.incrementAndGet();
                freeBuffers.add(b);
            } else {
                log().error("create buffer failed");
            }
        }
    }

    public void destroyBuffers() {
        for (var b : freeBuffers) {
            destroyBuffer(b);
        }
        freeBuffers.clear();
    }
}
