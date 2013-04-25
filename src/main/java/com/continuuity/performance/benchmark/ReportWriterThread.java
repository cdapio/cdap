package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportWriterThread extends ReportThread {

  private static final Logger LOG = LoggerFactory.getLogger(ReportWriterThread.class);

  private static final String OPS_PER_SEC_ONE_MIN = "benchmark.ops.per_sec.1m";
  private static final String OPS_PER_SEC_AVG = "benchmark.ops.per_sec.avg";

  String fileName;
  File reportFile;
  String benchmarkName;
  Map<Integer,ArrayList<Double>> mensaMetrics;

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
    mensaMetrics = new HashMap<Integer,ArrayList<Double>>(groups.length);
    for (int i=0; i<groups.length; i++) {
      mensaMetrics.put(i, new ArrayList<Double>());
    }
  }

  public void run() {
    BufferedWriter bw = null;
    long unixTime;
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
        unixTime = currentTime / 1000L;
        if (wakeup > currentTime) {
          long sleepTime = wakeup - currentTime;
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            LOG.debug("InterruptedException caught during Thread.sleep({}).", sleepTime);
            interrupt = true;
          }
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
                mensaMetrics.get(i).add(valueSince * 1000.0 / millisSince);
                String metricValue = String.format("%1.2f", valueSince * 1000.0 / millisSince);
                String metric = buildMetric(OPS_PER_SEC_ONE_MIN, Long.toString(unixTime), metricValue, benchmarkName,
                                            groups[i].getName(), Integer.toString(groups[i].getNumAgents()));
                LOG.info("Collected metric {} in memory ", metric);
              }
            }
          }
          previousMetrics.set(i, grpMetrics);
          previousMillis[i] = millis;
        }
      }
      LOG.debug("Summarizing collected metrics...");
      unixTime = System.currentTimeMillis() / 1000L;
      for (int i=0; i<groups.length; i++) {
        List<Double> grpVals = mensaMetrics.get(i);
        if (grpVals.size() != 0 ) {
          double sum = 0;
          for (int j = 0; j < grpVals.size(); j++) {
            sum += grpVals.get(j);
          }
          double avg = sum / grpVals.size();
          String metricValue = String.format("%1.2f", avg);
          String metric = buildMetric(OPS_PER_SEC_AVG, Long.toString(unixTime), metricValue, benchmarkName,
                                      groups[i].getName(), Integer.toString(groups[i].getNumAgents()));
          LOG.info("Writing {} to file {}", metric, fileName);
          bw.write(metric);
          bw.write("\n");
          bw.flush();
        }
      }
    } catch (IOException e) {
      LOG.error("Error when trying to write to report file " + fileName);
    } finally {
      if (bw != null) try { bw.close(); } catch (IOException e) {
        e.printStackTrace();
      }
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
    builder.append(" benchmark=");
    builder.append(benchmark);
    builder.append(" operation=");
    builder.append(operation);
    builder.append(" threadCount=");
    builder.append(threadCount);
    return builder.toString();
  }
}
