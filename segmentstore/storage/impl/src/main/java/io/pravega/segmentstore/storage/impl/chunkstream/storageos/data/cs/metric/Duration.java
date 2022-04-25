package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Duration implements Metric {
    private static final VarHandle MinHandle;
    private static final VarHandle MaxHandle;
    private static final VarHandle SumHandle;
    private static final VarHandle CountHandle;
    private static final VarHandle TotalSumHandle;
    private static final VarHandle TotalCountHandle;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            MinHandle = l.findVarHandle(Duration.class, "min", long.class);
            MaxHandle = l.findVarHandle(Duration.class, "max", long.class);
            SumHandle = l.findVarHandle(Duration.class, "sum", long.class);
            CountHandle = l.findVarHandle(Duration.class, "count", long.class);
            TotalSumHandle = l.findVarHandle(Duration.class, "totalSum", double.class);
            TotalCountHandle = l.findVarHandle(Duration.class, "totalCount", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    private volatile long min = Long.MAX_VALUE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long max = Long.MIN_VALUE;
    private volatile long sum;
    private volatile long count;
    private volatile double totalSum;
    private volatile long totalCount;

    public void updateNanoSecond(long duration) {
        updateMicroSecond(duration / 1000);
    }

    public void updateMicroSecond(long duration) {
        SumHandle.getAndAddRelease(this, duration);
        CountHandle.getAndAddRelease(this, 1);
        // not accurate, but enough as reference
        if (duration < (long) MinHandle.getOpaque(this)) {
            MinHandle.setOpaque(this, duration);
        }
        if (duration > (long) MaxHandle.getOpaque(this)) {
            MaxHandle.setOpaque(this, duration);
        }
    }

    public void updateMilliSecond(long duration) {
        updateMicroSecond(duration * 1000);
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) CountHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        var sum = (long) SumHandle.getOpaque(this);
        var min = (long) MinHandle.getOpaque(this);
        var max = (long) MaxHandle.getOpaque(this);

        TotalCountHandle.getAndAddRelease(this, count);
        CountHandle.getAndAddRelease(this, -count);
        MinHandle.weakCompareAndSetRelease(this, min, Long.MAX_VALUE);
        MaxHandle.weakCompareAndSetRelease(this, max, Long.MIN_VALUE);
        TotalSumHandle.getAndAddRelease(this, sum);
        SumHandle.getAndAddRelease(this, -sum);
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricDuration, name, count, (float) count / durationSeconds, sum / count, min, max));
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) TotalCountHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        var sum = (double) TotalSumHandle.getOpaque(this);
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricDuration, name, count, (float) count / durationSeconds, (long) (sum / count), 0, 0));
    }
}
