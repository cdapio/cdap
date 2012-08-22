package com.continuuity.common.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;

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
        for (ImmutablePair<String, Long> metric : groupMetrics[i].list()) {
          builder.append(sep); sep = ", ";
          builder.append(metric.getSecond());
          builder.append(' ');
          builder.append(metric.getFirst());
          builder.append(" (");
          builder.append(String.format(
              "%1.1f/sec, %1.1f/sec/thread)",
              metric.getSecond() * 1000.0 / millis,
              metric.getSecond() * 1000.0 / millis / groups[i].getNumAgents()
          ));
        }
        System.out.println(builder.toString());
      }
    }
  }

}
