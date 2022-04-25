package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Amount implements Metric {
    private static final VarHandle MinHandle;
    private static final VarHandle MaxHandle;
    private static final VarHandle AmountHandle;
    private static final VarHandle CountHandle;
    private static final VarHandle TotalAmountHandle;
    private static final VarHandle TotalCountHandle;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            MinHandle = l.findVarHandle(Amount.class, "min", double.class);
            MaxHandle = l.findVarHandle(Amount.class, "max", double.class);
            AmountHandle = l.findVarHandle(Amount.class, "amount", double.class);
            CountHandle = l.findVarHandle(Amount.class, "count", long.class);
            TotalAmountHandle = l.findVarHandle(Amount.class, "totalAmount", double.class);
            TotalCountHandle = l.findVarHandle(Amount.class, "totalCount", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    private volatile double min = Long.MAX_VALUE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile double max = Long.MIN_VALUE;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile double amount = 0.0;
    private volatile long count;
    private volatile double totalAmount;
    private volatile long totalCount;

    public void count(double amount) {
        AmountHandle.getAndAddRelease(this, amount);
        CountHandle.getAndAdd(this, 1);
        // not accurate, but enough as reference
        if (amount < (double) MinHandle.getOpaque(this)) {
            MinHandle.setOpaque(this, amount);
        }
        if (amount > (double) MaxHandle.getOpaque(this)) {
            MaxHandle.setOpaque(this, amount);
        }
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) CountHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        var min = (double) MinHandle.getOpaque(this);
        var max = (double) MaxHandle.getOpaque(this);
        var amount = (double) AmountHandle.getOpaque(this);
        MinHandle.weakCompareAndSetRelease(this, min, Long.MAX_VALUE);
        MaxHandle.weakCompareAndSetRelease(this, max, Long.MIN_VALUE);
        TotalAmountHandle.getAndAddRelease(this, amount);
        AmountHandle.getAndAddRelease(this, -amount);
        TotalCountHandle.getAndAddRelease(this, count);
        CountHandle.getAndAddRelease(this, -count);
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricAmount,
                                    name,
                                    StringUtils.size2String(amount),
                                    StringUtils.size2String(amount / durationSeconds),
                                    StringUtils.size2String(amount / count),
                                    StringUtils.size2String(min),
                                    StringUtils.size2String(max)));
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var count = (long) TotalCountHandle.getOpaque(this);
        if (count <= 0) {
            return;
        }
        var amount = (double) TotalAmountHandle.getOpaque(this);
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricAmount,
                                    name,
                                    StringUtils.size2String(amount),
                                    StringUtils.size2String(amount / durationSeconds),
                                    StringUtils.size2String(amount / count),
                                    "",
                                    ""));
    }
}
