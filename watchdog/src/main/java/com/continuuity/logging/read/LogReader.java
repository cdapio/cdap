package com.continuuity.logging.read;

import com.continuuity.common.logging.LoggingContext;

import java.util.concurrent.Future;

/**
 * Interface to read logs.
 */
public interface LogReader {
  /**
   * Read log events of a Flow, Procedure or Map Reduce program after a given offset.
   * @param loggingContext context to look up log events.
   * @param fromOffset offset after which to start reading.
   * @param maxEvents max log events to return.
   * @param callback callback to handle the log events.
   * @return Future of Runnable that reads the log events.
   */
  Future<?> getLogNext(LoggingContext loggingContext, long fromOffset, int maxEvents, Callback callback);

  /**
   * Read log events of a Flow, Procedure or Map Reduce program before a given offset.
   * @param loggingContext context to look up log events.
   * @param fromOffset offset before which to start reading.
   * @param maxEvents max log events to return.
   * @param callback callback to handle the log events.
   * @return Future of Runnable that reads the log events.
   */
  Future<?> getLogPrev(LoggingContext loggingContext, long fromOffset, int maxEvents, Callback callback);

  /**
   * Returns log events of a Flow, Procedure or Map between given times.
   * @param loggingContext context to look up log events.
   * @param fromTimeMs start time.
   * @param toTimeMs end time.
   * @param callback Callback to handle the log events.
   */
  Future<?> getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs, Callback callback);

  /**
   * Releases any resources associated with the reader.
   */
  void close();
}
