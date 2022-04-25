package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    private final String name;
    private final AtomicInteger threadNum = new AtomicInteger(1);

    public NamedForkJoinWorkerThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new NamedForkJoinWorkerThread(pool, name + threadNum.getAndIncrement());
    }

    private static class NamedForkJoinWorkerThread extends ForkJoinWorkerThread {
        /**
         * Creates a ForkJoinWorkerThread operating in the given pool.
         *
         * @param pool the pool this thread works in
         * @throws NullPointerException if pool is null
         */
        protected NamedForkJoinWorkerThread(ForkJoinPool pool, String name) {
            super(pool);
            this.setName(name);
        }
    }
}
