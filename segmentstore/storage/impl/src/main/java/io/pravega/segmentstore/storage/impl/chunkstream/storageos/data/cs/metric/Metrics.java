package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Metrics {
    static private final Logger log = LoggerFactory.getLogger(Metrics.class);
    static private final Metrics gMetrics = new Metrics();
    static private final int reportDurationSeconds = CSConfiguration.metricsReportSeconds;
    static char[] padding = new char[1024];
    static String formatReportHeader;
    static String formatReportMetricCount;
    static String formatReportMetricAmount;
    static String formatReportMetricDuration;
    static String formatReportMetricForkJoinPool;
    static String formatReportMetricNic;
    static String formatReportMetricTop;
    static String formatReportMetricC2KCache;

    static {
        Arrays.fill(padding, ' ');
    }

    private final Map<String, Metric> metrics = new TreeMap<>();
    private final Map<String, Metric> statsMetrics = new TreeMap<>();
    private ScheduledThreadPoolExecutor reporter;
    private int maxNameLen = 30;
    private boolean updateFormat = true;
    private long firstReportMs;
    private long lastReportMs;
    private volatile boolean haveFlushTotalReport;

    public static void start() {
        if (gMetrics.reporter != null) {
            log.warn("metrics reporter has been started");
            return;
        }
        gMetrics.firstReportMs = gMetrics.lastReportMs = System.currentTimeMillis();
        var periods = reportDurationSeconds * 1000;
        var delay = periods - gMetrics.lastReportMs % periods;
        gMetrics.reporter = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("metrics"));
        gMetrics.reporter.scheduleAtFixedRate(gMetrics::periodsReport, delay, periods, TimeUnit.MILLISECONDS);
    }

    public static void stop() {
        if (gMetrics.reporter != null) {
            gMetrics.reporter.shutdown();
            gMetrics.reporter = null;
        }
    }

    public static <M extends Metric> M makeMetric(String name, Class<M> metricClass) {
        return (M) gMetrics.register(name, metricClass);
    }

    public static void makeStatsMetric(String name, StatsMetric statsMetric) {
        gMetrics.registerStatsMetric(name, statsMetric);
    }

    public static boolean removeMetric(String name) {
        return gMetrics.unRegister(name);
    }

    public static boolean removeStatsMetric(String name) {
        return gMetrics.unRegisterStatsMetric(name);
    }

    public static void flushReport() {
        gMetrics.periodsReport();
        gMetrics.totalReport();
    }

    private <M extends Metric> Metric register(String name, Class<M> metricClass) {
        synchronized (metrics) {
            return metrics.computeIfAbsent(name, k -> {
                try {
                    if (name.length() > maxNameLen) {
                        maxNameLen = name.length();
                        updateFormat = true;
                    }
                    return metricClass.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    log.error("create metric class {} failed", metricClass.getName());
                    throw new CSRuntimeException("create metrics failed", e);
                }
            });
        }
    }

    private void registerStatsMetric(String name, StatsMetric statsMetric) {
        synchronized (statsMetrics) {
            statsMetrics.putIfAbsent(name, statsMetric);
        }
    }

    private boolean unRegister(String name) {
        synchronized (metrics) {
            return metrics.remove(name) != null;
        }
    }

    private boolean unRegisterStatsMetric(String name) {
        synchronized (statsMetrics) {
            return statsMetrics.remove(name) != null;
        }
    }

    private void periodsReport() {
        if (updateFormat) {
            updateReportFormat();
        }
        var indent = 4;
        var preReportTime = lastReportMs;
        lastReportMs = System.currentTimeMillis();
        var periods = (lastReportMs - preReportTime) / 1000.0f;
        StringBuffer r = new StringBuffer();
        r.append(Metrics.padding, 0, indent)
         .append(String.format(formatReportHeader, "name", "count", "tps/bw", "total/avg", "min", "max"));
        var before = r.length();
        for (var m : metrics.entrySet()) {
            m.getValue().periodsReport(r, m.getKey(), periods, indent);
        }
        if (r.length() - before > 128) {
            haveFlushTotalReport = false;
        }
        for (var m : statsMetrics.entrySet()) {
            m.getValue().periodsReport(r, m.getKey(), periods, indent);
        }
        log.info("periodsReport duration {}s: \n{}", String.format("%.2f", periods), r);
    }

    private void totalReport() {
        if (haveFlushTotalReport) {
            return;
        }
        if (updateFormat) {
            updateReportFormat();
        }
        var indent = 4;
        var periods = (lastReportMs - firstReportMs) / 1000.0f;
        StringBuffer r = new StringBuffer();
        r.append(Metrics.padding, 0, indent)
         .append(String.format(formatReportHeader, "name", "count", "tps/bw", "total/avg", "min", "max"));
        for (var m : metrics.entrySet()) {
            m.getValue().totalReport(r, m.getKey(), periods, indent);
        }
        for (var m : statsMetrics.entrySet()) {
            m.getValue().totalReport(r, m.getKey(), periods, indent);
        }
        log.info("totalReport duration {}s: \n{}", String.format("%.2f", periods), r);
        haveFlushTotalReport = true;
    }

    private void updateReportFormat() {
        var nameLen = maxNameLen + 2;
        formatReportHeader = "%-" + nameLen + "s %15s %15s %15s %15s %15s\n";
        formatReportMetricCount = "%-" + nameLen + "s %15d %15.2f %15d\n";
        formatReportMetricAmount = "%-" + nameLen + "s %15s %15s %15s %15s %15s\n";
        formatReportMetricDuration = "%-" + nameLen + "s %15d %15.2f %,15d %,15d %,15d\n";
        formatReportMetricForkJoinPool = "%-" + nameLen + "s %15s %s\n";
        formatReportMetricNic = "%-" + nameLen + "s %15s %15s %15s\n";
        formatReportMetricTop = "%-" + nameLen + "s %15s %s\n";
        formatReportMetricC2KCache = "%-" + nameLen + "s %15s %s\n";
    }
}
