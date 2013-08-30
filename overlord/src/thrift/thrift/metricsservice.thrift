namespace java com.continuuity.metrics2.thrift

/**
 * Level-1 Cleanup - kept the same interfaces, but removed unwanted stuff
 */

/**
 * Thrown when there is any issue that client should know about in MetricsService.
 */
exception MetricsServiceException {
  1:string message,
}

enum TEntityType {
  FLOW = 1,
  PROCEDURE = 2,
  MAP_REDUCE = 3,
}

/**
 * Log result
 */
struct TLogResult {
   1: required string logLine,
   2: required i64 offset,
}

/**
 * Metrics Service is a frontend service for retreiving metrics.
 */
service MetricsFrontendService {
  /**
   * Returns log line.
   */
  list<string> getLog(1: string accountId, 2: string applicationId,
                      3: string flowId, 4: i32 size)
    throws (1: MetricsServiceException e),

  /**
   * Returns log lines after given offset. -1 as fromOffset returns the latest log lines.
   */
  list<TLogResult> getLogNext(1: string accountId, 2: string applicationId, 3: string entityId,
                        4: TEntityType entityType, 5: i64 fromOffset, 6: i32 maxEvents, 7: string filter)
    throws (1: MetricsServiceException e),

  /**
   * Returns log lines before given offset. -1 as fromOffset returns the latest log lines.
   */
  list<TLogResult> getLogPrev(1: string accountId, 2: string applicationId, 3: string entityId,
                        4: TEntityType entityType, 5: i64 fromOffset, 6: i32 maxEvents, 7: string filter)
    throws (1: MetricsServiceException e),

}

