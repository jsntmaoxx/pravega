package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NicStats implements StatsMetric {
    static private final Logger log = LoggerFactory.getLogger(NicStats.class);
    static private final String TX_BYTES = "     tx_bytes:";
    static private final String RX_BYTES = "     rx_bytes:";
    protected final String deviceName;
    protected final ProcessBuilder processBuilder;
    private final long initRxBytes;
    private final long initTxBytes;
    private long lastRxBytes;
    private long lastTxBytes;
    private long rxBytes;
    private long txBytes;

    public NicStats(String deviceName) {
        this.deviceName = deviceName;
        this.processBuilder = NicStats.currentUser().equals("root") ? new ProcessBuilder().command("ethtool", "-S", this.deviceName)
                                                                    : new ProcessBuilder().command("sudo", "ethtool", "-S", this.deviceName);
        refresh();
        this.initRxBytes = this.lastRxBytes = this.rxBytes;
        this.initTxBytes = this.lastTxBytes = this.txBytes;
    }

    public static String currentUser() {
        try {
            var builder = new ProcessBuilder().command("whoami");
            var p = builder.start();
            var reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            var line = reader.readLine();
            reader.close();
            return line.trim();
        } catch (Exception e) {
            log.error("check root user failed", e);
        }
        return "unknown";
    }

    public static List<String> nicNames() {
        var names = new ArrayList<String>(4);
        try {
            var builder = new ProcessBuilder().command("ip", "link", "show");
            var p = builder.start();
            var reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.contains("state UP")) {
                    continue;
                }
                var parts = line.split(":");
                if (parts.length > 1) {
                    var nic = parts[1].trim();
                    names.add(nic);
                    log.info("find nic {}", nic);
                }
            }
            p.waitFor(1, TimeUnit.SECONDS);
            reader.close();
        } catch (Exception e) {
            if (e.getCause() != null && e.getCause().getMessage().contains("error=2")) {
                log.warn("failed to get nic names due to have no ip command");
            } else {
                log.error("fetch nic names failed", e);
            }
        }
        return names;
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        refresh();
        if (lastRxBytes == rxBytes && this.lastTxBytes == txBytes) {
            return;
        }
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricNic, name + ".rx.tx",
                                    "",
                                    StringUtils.size2String((this.rxBytes - this.lastRxBytes) / durationSeconds),
                                    StringUtils.size2String((this.txBytes - this.lastTxBytes) / durationSeconds)));
        this.lastRxBytes = rxBytes;
        this.lastTxBytes = txBytes;
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricNic, name + ".rx.tx",
                                    "",
                                    StringUtils.size2String((this.rxBytes - this.initRxBytes) / durationSeconds),
                                    StringUtils.size2String((this.txBytes - this.initTxBytes) / durationSeconds)));
    }

    private void refresh() {
        Process p;
        try {
            p = processBuilder.start();
            var reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(rxPrefix())) {
                    rxBytes = Long.parseLong(line.split(":")[1].trim());
                } else if (line.startsWith(txPrefix())) {
                    txBytes = Long.parseLong(line.split(":")[1].trim());
                }
            }
            p.waitFor(1, TimeUnit.SECONDS);
            reader.close();
        } catch (Exception e) {
            log.error("query mlx nic {} status failed. Command {}", deviceName, processBuilder.command(), e);
        }
    }

    protected String rxPrefix() {
        return RX_BYTES;
    }

    protected String txPrefix() {
        return TX_BYTES;
    }
}
