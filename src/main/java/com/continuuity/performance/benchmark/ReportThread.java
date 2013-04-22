package com.continuuity.performance.benchmark;

public abstract class ReportThread extends Thread {

  int reportInterval = 60;
  AgentGroup[] groups;
  BenchmarkMetric[] groupMetrics;

  public abstract void run();

}
