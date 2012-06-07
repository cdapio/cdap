namespace java com.continuuity.metrics.stubs

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

struct FlowEvent {
  1: i32 startTime,
  2: i32 endTime,
  3: string accountId,
  4: string app,
  5: string flow,
  6: string rid,
  7: i32 state,
}


enum MetricType {
  FLOWLET = 1
}

struct Metric {
  1: string id,
  2: MetricType type,
  3: string name,
  4: i64 value,
}

struct FlowState {
  1: string application,
  2: string flow,
  3: i32 lastStoppped,
  4: i32 lastStarted,
  5: i32 currentState,
  6: i32 runs,
}

service FlowMonitor {
    void add(1: FlowMetric metric),
    list<FlowState> getFlows(1: string accountId),
    list<FlowEvent> getFlowHistory(1: string accountId, 2: string appid, 3: string flowId),
    list<Metric> getFlowMetrics(1: string accountId, 2: string appId, 3: string flowId, 4: string rid),
}
