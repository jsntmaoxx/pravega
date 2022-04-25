package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;


import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class G {
    private static final AtomicLong requestId = new AtomicLong(0);
    private static final AtomicLong batchRequestId = new AtomicLong(0);
    private static final AtomicLong ecRequestId = new AtomicLong(0);
    public static Unsafe U = null;
    public static long INT_ARRAY_BASE_OFFSET;
    public static long INT_ARRAY_INDEX_SCALE;
    public static long BYTE_ARRAY_BASE_OFFSET;
    public static long BYTE_ARRAY_INDEX_SCALE;
    public static UUID ZeroUUID = new UUID(0L, 0L);

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            U = (Unsafe) f.get(null);
            INT_ARRAY_BASE_OFFSET = U.arrayBaseOffset(int[].class);
            INT_ARRAY_INDEX_SCALE = U.arrayIndexScale(int[].class);
            BYTE_ARRAY_BASE_OFFSET = U.arrayBaseOffset(byte[].class);
            BYTE_ARRAY_INDEX_SCALE = U.arrayIndexScale(byte[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            System.out.println("init unsafe failed");
        }
    }

    public static long genRequestId() {
        return requestId.incrementAndGet();
    }

    public static long genBatchRequestId() {
        return batchRequestId.incrementAndGet();
    }

    public static long genECRequestId() {
        return ecRequestId.incrementAndGet();
    }
}
