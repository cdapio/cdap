package com.continuuity.common.benchmark;

import com.continuuity.common.utils.ImmutablePair;

public class ReportThread extends Thread {

  AgentGroup[] groups;
  BenchmarkMetric[] groupMetrics;

  public ReportThread(AgentGroup[] groups, BenchmarkMetric[] metrics) {
    this.groupMetrics = metrics;
    this.groups = groups;
  }

  public void run() {
    long start = System.currentTimeMillis();
    boolean interrupted = false;
    StringBuilder builder = new StringBuilder();
    // wake up every minute to report the metrics
    for (int seconds = 1; !interrupted; ++seconds) {
      long wakeup = start + (seconds * 1000);
      try {
        Thread.sleep(wakeup - System.currentTimeMillis());
      } catch (InterruptedException e) {
        interrupted = true;
      }
      double time = !interrupted ? seconds :
          ((System.currentTimeMillis() - start) / 1000.0);
      System.out.println("After " + time + " seconds: ");
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
          builder.append(String.format("%1.1f", metric.getSecond() / time));
          builder.append("/sec)");
        }
        System.out.println(builder.toString());
      }
    }
  }

}
