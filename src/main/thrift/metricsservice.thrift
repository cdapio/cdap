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
  4: optional string flowletId,
  5: optional i32 instanceId,
}

/**
 * Defines a request to be made to server.
 */
struct CounterRequest {
  1: FlowArgument argument,
  2: optional list<string> name,
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
}
