package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Count implements Metric {
    private static final VarHandle CountHandle;
    private static final VarHandle TotalHandle;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CountHandle = l.findVarHandle(Count.class, "count", long.class);
            TotalHandle = l.findVarHandle(Count.class, "total", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile long count;
    private volatile long total;

    public void increase() {
        CountHandle.getAndAddRelease(this, 1);
    }

    public void increase(long delta) {
        CountHandle.getAndAddRelease(this, delta);
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) CountHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        TotalHandle.getAndAddRelease(this, count);
        var total = (long) TotalHandle.getOpaque(this);
        CountHandle.getAndAddRelease(this, -count);
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricCount, name, count, (float) count / durationSeconds, total));
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) TotalHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricCount, name, count, (float) count / durationSeconds, count));
    }
}
