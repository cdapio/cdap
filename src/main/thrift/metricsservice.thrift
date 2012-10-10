namespace java com.continuuity.metrics2.stubs

/**
 * Specifies the counter by it's name and it's value.
 */
struct Counter {
  1: string qualifier,
  2: string name,
  3: double value,
}

/**
 * Thrown when there is any issue that client should know about in MetricsService.
 */
exception MetricsServiceException {
  1:string message,
}

struct FlowArgument {
  1: string accountId,
  2: string applicationId,
  3: string flowId,
  4: optional string runId,
  5: optional string flowletId,
  6: optional i32 instanceId,
}

/**
 * Defines a request to be made to server.
 */
struct CounterRequest {
  1: FlowArgument argument,
  2: optional list<string> name,
}

/**
 * Point in time.
 */
struct DataPoint {
  1: i64 timestamp,
  2: double value
}

enum MetricTimeseriesLevel {
  ACCOUNT_LEVEL = 1,
  APPLICATION_LEVEL = 2,
  FLOW_LEVEL = 3,
  FLOWLET_LEVEL = 4,
  RUNID_LEVEL = 5,
}

/**
 * Collection of data points for a given metric.
 */
struct DataPoints {
   1: map<string, list<DataPoint>> points,
   2: map<string, double> latest,
}

/**
 * Timeseries request
 */
struct TimeseriesRequest {
   1: required FlowArgument argument,
   2: required list<string> metrics,
   3: optional MetricTimeseriesLevel level,
   4: optional i64 startts,
   5: required i64 endts,
   6: optional bool summary = 1,
}

/**
 * Metrics Service is a frontend service for retreiving metrics.
 */
service MetricsFrontendService {

 /**
  * Returns the requested counter for a given account, application, flow
  * & run. All the counter for the combination could be retrieved by specifying
  * ALL in the metric name.
  */
  list<Counter> getCounters(1: CounterRequest request)
    throws (1: MetricsServiceException e),

 /**
  * API to request time series data for a set of metrics.
  */
  DataPoints getTimeSeries(1: TimeseriesRequest request)
    throws (1: MetricsServiceException e),
}

