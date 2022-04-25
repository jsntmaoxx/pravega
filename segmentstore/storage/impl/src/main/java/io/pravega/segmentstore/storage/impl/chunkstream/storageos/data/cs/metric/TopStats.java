package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class TopStats implements StatsMetric {
    static private final Logger log = LoggerFactory.getLogger(TopStats.class);

    private final ProcessBuilder processBuilder;
    private final String pid;
    private String loadAvg;
    private String totalCpu;
    private String totalMem;
    private String totalSwap;
    private String res;
    private String cpu;

    public TopStats() {
        pid = String.valueOf(ProcessHandle.current().pid());
        // "-H" to statistic hot threads
        this.processBuilder = new ProcessBuilder().command("top", "-p", pid, "-b", "-n", "1");
    }

    @Override
    public void periodsReport(StringBuffer report, String name, float durationSeconds, int indent) {
        refresh();
        report.append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + ".load", "", loadAvg))
              .append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + ".cpu", "", totalCpu))
              .append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + ".mem", "", totalMem))
              .append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + ".swap", "", totalSwap))
              .append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + "." + pid + ".cpu", "", cpu))
              .append(Metrics.padding, 0, indent)
              .append(String.format(Metrics.formatReportMetricTop, name + "." + pid + ".res", "", res));
    }

    @Override
    public void totalReport(StringBuffer report, String name, float durationSeconds, int indent) {

    }

    private void refresh() {
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = reader.readLine();
            loadAvg = line.substring(line.indexOf("load average"));
            reader.readLine();
            line = reader.readLine();
            totalCpu = line.substring(line.indexOf(":") + 1).trim();
            line = reader.readLine();
            while (!line.contains("Mem")) {
                line = reader.readLine();
                if (line == null) {
                    throw new CSException("no Mem info on top");
                }
            }
            totalMem = line.substring(line.indexOf(":") + 1).trim();
            line = reader.readLine();
            while (!line.contains("Swap")) {
                line = reader.readLine();
                if (line == null) {
                    throw new CSException("no Swap info on top");
                }
            }
            totalSwap = line.substring(line.indexOf(":") + 1).trim();
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith(pid)) {
                    var parts = line.split("\\s+");
                    res = parts[5];
                    cpu = parts[8];
                    break;
                }
            }
            p.waitFor(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("query top status failed. Command {}", processBuilder.command());
        }
    }
}
