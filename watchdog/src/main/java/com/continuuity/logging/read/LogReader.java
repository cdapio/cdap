package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContext;
import com.google.common.base.Objects;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Interface to read logs.
 */
public interface LogReader {
  /**
   * Returns log events of a Flow, Procedure or Map Reduce program after given position.
   * @param loggingContext context to look up log events.
   * @param positionHint position hint from the previous call, if any.
   * @param maxEvents max log events to return.
   * @return log events list and position hint.
   */
  Result getLogNext(LoggingContext loggingContext, String positionHint, int maxEvents);

  /**
   * Returns log events of a Flow, Procedure or Map Reduce program before given position.
   * @param loggingContext context to look up log events.
   * @param positionHint position hint from the previous call, if any.
   * @param maxEvents max log events to return.
   * @return log events list and position hint.
   */
  Result getLogPrev(LoggingContext loggingContext, String positionHint, int maxEvents);

  Future<?> getLogNext(LoggingContext loggingContext, long fromOffset, int maxEvents, Callback callback);

  Future<?> getLogPrev(LoggingContext loggingContext, long fromOffset, int maxEvents, Callback callback);

  /**
   * Returns log events of a Flow, Procedure or Map between given times.
   * @param loggingContext context to look up log events.
   * @param fromTimeMs start time.
   * @param toTimeMs end time.
   * @param callback Callback to handle the log events.
   */
  Future<?> getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs, Callback callback);

  void close();

  /**
   * Result of reading logs. Contains list of log events, position hint and a flag that indicates if the events are
   * incremental to the given position hint.
   */
  public final class Result {
    private final List<ILoggingEvent> loggingEvents;
    private final String positionHint;
    private final boolean incremental;

    public Result(List<ILoggingEvent> loggingEvents, String positionHint, boolean incremental) {
      this.loggingEvents = loggingEvents;
      this.positionHint = positionHint;
      this.incremental = incremental;
    }

    public List<ILoggingEvent> getLoggingEvents() {
      return loggingEvents;
    }

    public String getPositionHint() {
      return positionHint;
    }

    public boolean isIncremental() {
      return incremental;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("loggingEvents", loggingEvents)
        .add("positionHint", positionHint)
        .add("incremental", incremental)
        .toString();
    }
  }
}
