package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import org.slf4j.Logger;

public class DefaultExceptionHandler implements Thread.UncaughtExceptionHandler {
    private final Logger log;

    public DefaultExceptionHandler(Logger log) {
        this.log = log;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("unhandled exception", e);
    }
}
