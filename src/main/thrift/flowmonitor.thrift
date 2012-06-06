namespace java com.continuuity.overlord.metrics.stubs

struct FlowMetric {
  1: i32 timestamp,
  2: string accountId,
  3: string application,
  4: string flow,
  5: string rid,
  6: string version,
  7: string flowlet,
  8: string instance,
  9: string metric,
  10: i64 value,
}

service FlowMonitor {
    void add(1: FlowMetric metric),
}
