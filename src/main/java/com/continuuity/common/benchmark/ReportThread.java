package com.continuuity.common.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.Map;

public class ReportThread extends Thread {

  int reportInterval = 60;
  AgentGroup[] groups;
  BenchmarkMetric[] groupMetrics;

  public ReportThread(AgentGroup[] groups, BenchmarkMetric[] metrics,
                      CConfiguration config) {
    this.groupMetrics = metrics;
    this.groups = groups;
    this.reportInterval = config.getInt("report", reportInterval);
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
    Map<String, Long> previousMetrics = null;
    long previousMillis = 0;
    // wake up every minute to report the metrics
    for (int seconds = reportInterval; !interrupt; seconds += reportInterval) {
      long wakeup = start + (seconds * 1000);
      try {
        Thread.sleep(wakeup - System.currentTimeMillis());
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
        for (Map.Entry<String, Long> kv : metrics.entrySet()) {
          String key = kv.getKey();
          long value = kv.getValue();
          builder.append(sep); sep = ", ";
          builder.append(String.format(
              "%d %s (%1.1f/sec, %1.1f/sec/thread)",
              value, key,
              value * 1000.0 / millis,
              value * 1000.0 / millis / numThreads
          ));
          if (previousMetrics != null) {
            Long previousValue = previousMetrics.get(key);
            if (previousValue == null) previousValue = 0L;
            long valueSince = value - previousValue;
            long millisSince = millis - previousMillis;
            builder.append(String.format(
                ", %d since last (%1.1f/sec, %1.1f/sec/thread)",
                valueSince,
                valueSince * 1000.0 / millisSince,
                valueSince * 1000.0 / millisSince / numThreads
            ));
          }
        }
        System.out.println(builder.toString());
        previousMetrics = metrics;
        previousMillis = millis;
      }
    }
  }

}
