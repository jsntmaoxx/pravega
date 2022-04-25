package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

public interface Metric {
    void periodsReport(StringBuffer report, String name, float durationSeconds, int indent);

    void totalReport(StringBuffer report, String name, float durationSeconds, int indent);
}
