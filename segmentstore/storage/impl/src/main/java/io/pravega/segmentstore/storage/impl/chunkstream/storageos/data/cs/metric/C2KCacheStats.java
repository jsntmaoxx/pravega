package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import org.cache2k.Cache;
import org.cache2k.operation.CacheControl;

public class C2KCacheStats implements StatsMetric {
    private final Cache<?, ?> cache;
    private long preGet;
    private long preMiss;
    private long prePut;
    private long preEvicted;
    private long preExpired;

    public C2KCacheStats(Cache<?, ?> cache) {
        this.cache = cache;
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var statistics = this.cache.requestInterface(CacheControl.class).sampleStatistics();
        var get = statistics.getGetCount();
        var miss = statistics.getMissCount();
        var put = statistics.getPutCount();
        var evicted = statistics.getEvictedCount();
        var expired = statistics.getExpiredCount();

        var dGet = (get - preGet);
        var dMiss = (miss - preMiss);
        var dPut = (put - prePut);
        var dEvicted = (evicted - preEvicted);
        var dExpired = (expired - preExpired);
        if (dGet == 0 && dMiss == 0 && dPut == 0 && dEvicted == 0 && dExpired == 0) {
            return;
        }

        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricC2KCache, name, "",
                                    "get " + dGet +
                                    " miss " + dMiss +
                                    " hit rate " + String.format("%.2f%%", (dGet > 0 ? (1.0 - (double) dMiss / dGet) * 100 : 0.0)) +
                                    " put " + dPut +
                                    " evicted " + dEvicted +
                                    " expired " + dExpired));
        preGet = get;
        preMiss = miss;
        prePut = put;
        preEvicted = evicted;
        preExpired = expired;
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
        var statistics = this.cache.requestInterface(CacheControl.class).sampleStatistics();
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricC2KCache, name, "",
                                    "get " + statistics.getGetCount() +
                                    " miss " + statistics.getMissCount() +
                                    " hit rate " + String.format("%.2f%%", statistics.getHitRate()) +
                                    " put " + statistics.getPutCount() +
                                    " evicted " + statistics.getEvictedCount() +
                                    " expired " + statistics.getExpiredCount()));
    }
}
