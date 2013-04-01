package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.util.MetricsHBaseUploader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ReportThread extends Thread {
  private static final int UPLOAD_TIMEOUT = 10*60*1000; // 10 min

  int reportInterval = 60;
  AgentGroup[] groups;
  String[] groupBenchmarkRunIds;
  BenchmarkMetric[] groupMetrics;
  String reportURL;

  public ReportThread(AgentGroup[] groups, BenchmarkMetric[] metrics,
                      CConfiguration config) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.reportInterval = config.getInt("report", reportInterval);
    this.reportURL = config.get("reporturl");
    String benchmarkName = config.get("bench");
    String buildName = config.get("build");
    if (buildName == null || buildName.length() == 0) {
      buildName = "nobuild";
    }
    this.groupBenchmarkRunIds = new String[groups.length];
    DateTimeZone zone = DateTimeZone.forID("US/Pacific");
    DateTime dt = new DateTime(zone);
    String prefix = dt.toLocalDate().toString("yyyy-MM-dd")
      +"_"+dt.toLocalTime().toString("HH:mm:ss")
      +"_"+buildName
      +"_"+benchmarkName;
    for (int i=0; i<groups.length; i++) {
      this.groupBenchmarkRunIds[i] = prefix + "_" + groups[i].getName()+ "_x" + groups[i].getNumAgents();
    }
  }

  String time2String(long millis) {
    final long second = 1000;
    final long minute = 60 * second;
    final long hour = 60 * minute;
    final long day = 24 * hour;

    StringBuilder builder = new StringBuilder();
    if (millis > day) {
      long days = millis / day;
      millis = millis % day;
      builder.append(days);
      builder.append('d');
    }
    long hours = millis / hour;
    millis = millis % hour;
    long minutes = millis / minute;
    millis = millis % minute;
    long seconds = millis / second;
    millis = millis % second;

    builder.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (millis > 0) builder.append(String.format(".%03d", millis));
    return builder.toString();
  }

  public void run() {
    long start = System.currentTimeMillis();
    boolean interrupt = false;
    StringBuilder builder = new StringBuilder();
    ArrayList<Map<String, Long>> previousMetrics =
        new ArrayList<Map<String, Long>>(groups.length);
    for (int i = 0; i < groups.length; i++)
      previousMetrics.add(null);
    long[] previousMillis = new long[groups.length];
    // wake up every minute to report the metrics
    for (int seconds = reportInterval; !interrupt; seconds += reportInterval) {
      long wakeup = start + (seconds * 1000);
      long currentTime = System.currentTimeMillis();
      try {
        if (wakeup > currentTime) Thread.sleep(wakeup - currentTime);
      } catch (InterruptedException e) {
        interrupt = true;
      }
      long millis =
          !interrupt ? seconds * 1000L : System.currentTimeMillis() - start;
      System.out.println(time2String(millis) + " elapsed: ");
      for (int i = 0; i < groups.length; i++) {
        builder.setLength(0);
        builder.append("Group " );
        builder.append(groups[i].getName());
        String sep = ": ";
        int numThreads = groups[i].getNumAgents();
        Map<String, Long> metrics = groupMetrics[i].list();
        Map<String, Long> prev = previousMetrics.get(i);
        for (Map.Entry<String, Long> kv : metrics.entrySet()) {
          String key = kv.getKey();
          long value = kv.getValue();
          double perSecRuns = value * 1000.0d / millis;
          double perSecPerThreadRuns = value * 1000.0d / millis / numThreads;
          if (!interrupt) {
            int minutes = (int) Math.round(millis / 1000.0d / 60.0d);
            if (this.reportURL != null) {
              try {
                MetricsHBaseUploader.uploadMetric(this.reportURL, this.groupBenchmarkRunIds[i],
                                                  "totalPerSec", minutes, perSecRuns);
                MetricsHBaseUploader.uploadMetric(this.reportURL, this.groupBenchmarkRunIds[i],
                                                  "totalPerSecPerThread", minutes, perSecPerThreadRuns);
              } catch (IOException e) {
                System.err.println("Could not upload benchmark metrics to HBase!");
              }
            }
          }
          builder.append(sep); sep = ", ";
          builder.append(String.format(
              "%d %s (%1.1f/sec, %1.1f/sec/thread)",
              value, key,
              perSecRuns,
              perSecPerThreadRuns
          ));
          if (!interrupt && prev != null) {
            Long previousValue = prev.get(key);
            if (previousValue == null) previousValue = 0L;
            long valueSince = value - previousValue;
            long millisSince = millis - previousMillis[i];
            builder.append(String.format(
                ", %d since last (%1.1f/sec, %1.1f/sec/thread)",
                valueSince,
                valueSince * 1000.0 / millisSince,
                valueSince * 1000.0 / millisSince / numThreads
            ));
          }
        }
        System.out.println(builder.toString());
        previousMetrics.set(i, metrics);
        previousMillis[i] = millis;
      }
    }
  }
}
