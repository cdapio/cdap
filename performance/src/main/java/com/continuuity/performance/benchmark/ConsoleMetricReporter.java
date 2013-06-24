package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;

import java.util.Map;

/**
 * Metrics collector that reports metrics on console.
 */
class ConsoleMetricReporter extends MetricsCollector {

  private int reportInterval = 60;

  protected ConsoleMetricReporter(AgentGroup[] groups, BenchmarkMetric[] metrics, CConfiguration config) {
    super(groups, metrics);
    this.reportInterval = config.getInt("report", reportInterval);
  }

  @Override
  protected int getInterval() {
    return reportInterval;
  }

  @Override
  protected void processGroupMetricsInterval(long unixTime, AgentGroup group, long previousMillis, long millis,
                                          Map<String, Long> prevMetrics, Map<String, Long> latestMetrics,
                                          boolean interrupt) {
    StringBuilder builder = new StringBuilder();
    builder.setLength(0);
    builder.append("Group ");
    builder.append(group.getName());
    String sep = ": ";

    for (Map.Entry<String, Long> kv : latestMetrics.entrySet()) {
      String key = kv.getKey();
      long value = kv.getValue();
      builder.append(sep);
      sep = ", ";
      builder.append(String.format("%d %s (%1.1f/sec, %1.1f/sec/thread)", value, key, value * 1000.0 / millis,
                                   value * 1000.0 / millis / group.getNumAgents()));
      if (!interrupt && prevMetrics != null) {
        Long previousValue = prevMetrics.get(key);
        if (previousValue == null) {
          previousValue = 0L;
        }
        long valueSince = value - previousValue;
        long millisSince = millis - previousMillis;
        builder.append(String.format(", %d since last (%1.1f/sec, %1.1f/sec/thread)", valueSince,
                                     valueSince * 1000.0 / millisSince,
                                     valueSince * 1000.0 / millisSince / group.getNumAgents()));
      }
    }
    System.out.println(builder.toString());
  }

  @Override
  protected void processGroupMetricsFinal(long unixTime, AgentGroup group) {
  }

  @Override
  protected void init() {
  }

  @Override
  protected void shutdown() {
  }
}
