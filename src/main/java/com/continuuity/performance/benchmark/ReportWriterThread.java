package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReportWriterThread extends ReportThread {

  private static final Logger Log = LoggerFactory.getLogger(ReportWriterThread.class);

  private static final String metricBaseName = "benchmark.ops.per_sec";

  String fileName;
  File reportFile;
  String benchmarkName;

  public ReportWriterThread(String benchmarkName, AgentGroup[] groups, BenchmarkMetric[] metrics, CConfiguration config) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.reportInterval = config.getInt("report", reportInterval);
    this.fileName = config.get("reportfile");
    int pos=benchmarkName.lastIndexOf(".");
    if (pos!=-1) {
      this.benchmarkName=benchmarkName.substring(pos + 1, benchmarkName.length());
    } else {
      this.benchmarkName=benchmarkName;
    }

  }

  public void run() {
   BufferedWriter bw = null;
    try {
      reportFile = new File(fileName);
      bw = new BufferedWriter(new FileWriter(reportFile));
      long start = System.currentTimeMillis();
      boolean interrupt = false;
      List<Map<String, Long>> previousMetrics = new ArrayList<Map<String, Long>>(groups.length);
      for (int i = 0; i < groups.length; i++) {
        previousMetrics.add(null);
      }
      long[] previousMillis = new long[groups.length];
      // wake up every minute to report the metrics
      for (int seconds = reportInterval; !interrupt; seconds += reportInterval) {
        long wakeup = start + (seconds * 1000);
        long currentTime = System.currentTimeMillis();
        long unixTime = currentTime / 1000L;
        try {
          if (wakeup > currentTime) {
            Thread.sleep(wakeup - currentTime);
          }
        } catch (InterruptedException e) {
          interrupt = true;
        }
        long millis;
        if (!interrupt) {
          millis = seconds * 1000L;
        } else {
          millis = System.currentTimeMillis() - start;
        }
        for (int i = 0; i < groups.length; i++) {
          Map<String, Long> grpMetrics = groupMetrics[i].list();
          Map<String, Long> prevGrpMetrics = previousMetrics.get(i);
          if (prevGrpMetrics != null) {
            for (Map.Entry<String, Long> singleMetric : grpMetrics.entrySet()) {
              String key = singleMetric.getKey();
              long value = singleMetric.getValue();
              if (!interrupt) {
                Long previousValue = prevGrpMetrics.get(key);
                if (previousValue == null) previousValue = 0L;
                long valueSince = value - previousValue;
                long millisSince = millis - previousMillis[i];
                String metricValue = String.format("%1.1f", valueSince * 1000.0 / millisSince);
                String metric = buildMetric(metricBaseName, Long.toString(unixTime), metricValue, benchmarkName,
                                            groups[i].getName(), Integer.toString(groups[i].getNumAgents()));
                Log.info("Writing "+metric+" to file "+fileName);
                bw.write(metric);
                bw.write("\n");
              }
            }
          }
          previousMetrics.set(i, grpMetrics);
          previousMillis[i] = millis;
        }
      }
    } catch (IOException e) {
      Log.error("Error when trying to write to report file " + fileName);
    } finally {
      if (bw != null) try { bw.close(); } catch (IOException e) { }
    }
  }
  private String buildMetric(String metric, String timestamp, String value, String benchmark, String operation,
                             String threadCount) {
    StringBuilder builder = new StringBuilder();
    builder.append("put");
    builder.append(" ");
    builder.append(metric);
    builder.append(" ");
    builder.append(timestamp);
    builder.append(" ");
    builder.append(value);
    // add tags to metric
    builder.append(" benchmarkName=");
    builder.append(benchmark);
    builder.append(" opName=");
    builder.append(operation);
    builder.append(" threadCount=");
    builder.append(threadCount);
    return builder.toString();
  }
}
