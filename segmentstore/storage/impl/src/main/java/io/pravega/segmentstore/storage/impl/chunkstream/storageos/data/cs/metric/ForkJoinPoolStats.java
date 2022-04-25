package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import java.util.concurrent.ForkJoinPool;

public class ForkJoinPoolStats implements StatsMetric {
    private final ForkJoinPool executor;

    public ForkJoinPoolStats(ForkJoinPool executor) {
        this.executor = executor;
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var stat = executor.toString();
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricForkJoinPool, name, "", stat.substring(stat.indexOf('[') + 1, stat.lastIndexOf(']'))));
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
    }
}
