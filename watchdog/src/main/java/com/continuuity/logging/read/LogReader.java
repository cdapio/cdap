package com.continuuity.logging.read;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.filter.Filter;

/**
 * Interface to read logs.
 */
public interface LogReader {
  /**
   * Read log events of a Flow, Procedure or Map Reduce program after a given offset.
   * @param loggingContext context to look up log events.
   * @param fromOffset offset after which to start reading. -1 to get the latest log events.
   * @param maxEvents max log events to return.
   * @param filter filter to select log events
   * @param callback callback to handle the log events.
   */
  void getLogNext(LoggingContext loggingContext, long fromOffset, int maxEvents, Filter filter,
                       Callback callback);

  /**
   * Read log events of a Flow, Procedure or Map Reduce program before a given offset.
   * @param loggingContext context to look up log events.
   * @param fromOffset offset before which to start reading. -1 to get the latest log events.
   * @param maxEvents max log events to return.
   * @param filter filter to select log events
   * @param callback callback to handle the log events.
   */
  void getLogPrev(LoggingContext loggingContext, long fromOffset, int maxEvents, Filter filter,
                       Callback callback);

  /**
   * Returns log events of a Flow, Procedure or Map between given times.
   * @param loggingContext context to look up log events.
   * @param fromTimeMs start time.
   * @param toTimeMs end time.
   * @param filter filter to select log events
   * @param callback Callback to handle the log events.
   */
  void getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs, Filter filter,
                   Callback callback);

  /**
   * Releases any resources associated with the reader.
   */
  void close();
}
