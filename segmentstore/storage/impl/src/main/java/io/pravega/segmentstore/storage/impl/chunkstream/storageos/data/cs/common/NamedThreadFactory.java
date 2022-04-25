package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final AtomicInteger threadNum = new AtomicInteger(1);

    public NamedThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name + threadNum.getAndIncrement());
    }
}
