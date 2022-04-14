package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

import java.time.Duration;

import static io.pravega.shared.MetricsTags.containerTag;

/**
 * Metrics for FileSystem.
 */
public class FileSystemMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("filesystem");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    /**
     * FileSystemLog-specific (i.e. per Container) Metrics.
     */
    final static class FileSystemLog implements AutoCloseable {

        private final OpStatsLogger writeQueueSize;
        private final OpStatsLogger writeQueueFillRate;
        private final OpStatsLogger writeLatency;
        private final OpStatsLogger totalWriteLatency;
        private final Counter fsWriteBytes;
        private final String[] containerTag;

        FileSystemLog(int containerId) {
            this.containerTag = containerTag(containerId);
            this.writeQueueSize = STATS_LOGGER.createStats(MetricsNames.FS_WRITE_QUEUE_SIZE, this.containerTag);
            this.writeQueueFillRate = STATS_LOGGER.createStats(MetricsNames.FS_WRITE_QUEUE_FILL_RATE, this.containerTag);
            this.writeLatency = STATS_LOGGER.createStats(MetricsNames.FS_WRITE_LATENCY, this.containerTag);
            this.totalWriteLatency = STATS_LOGGER.createStats(MetricsNames.FS_TOTAL_WRITE_LATENCY, this.containerTag);
            this.fsWriteBytes = STATS_LOGGER.createCounter(MetricsNames.FS_WRITE_BYTES, this.containerTag);
        }

        @Override
        public void close() {
            this.writeQueueSize.close();
            this.writeQueueFillRate.close();
            this.writeLatency.close();
            this.totalWriteLatency.close();
            this.fsWriteBytes.close();
        }

        void fileCount(int count) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.FS_FILE_COUNT, count, this.containerTag);
        }

        void queueStats(QueueStats qs) {
            this.writeQueueSize.reportSuccessValue(qs.getSize());
            this.writeQueueFillRate.reportSuccessValue((int) (qs.getAverageItemFillRatio() * 100));
        }

        void writeCompleted(Duration elapsed) {
            this.totalWriteLatency.reportSuccessEvent(elapsed);
        }

        void fileSystemWriteCompleted(int length, Duration elapsed) {
            this.writeLatency.reportSuccessEvent(elapsed);
            this.fsWriteBytes.add(length);
        }
    }
}
