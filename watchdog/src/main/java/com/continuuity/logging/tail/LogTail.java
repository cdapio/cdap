package com.continuuity.logging.tail;

import java.io.Closeable;

/**
 * Interface to tail logs.
 */
public interface LogTail extends Closeable {
  /**
   * Method to tail the log of a Flow.
   * @param accountId account Id of the Flow.
   * @param applicationId application Id of the Flow.
   * @param flowId Id of the Flow.
   * @param fromTimeMs time from when to start tailing.
   * @param callback Callback interface to receive logging event.
   */
  void tailFlowLog(String accountId, String applicationId, String flowId, long fromTimeMs, int maxEvents,
                   Callback callback);
}
