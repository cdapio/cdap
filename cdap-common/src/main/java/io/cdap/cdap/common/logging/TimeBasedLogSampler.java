/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.logging;

import co.cask.cdap.common.utils.TimeProvider;
import org.slf4j.spi.LocationAwareLogger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link LogSampler} that tracks and sample log events based on time.
 */
public abstract class TimeBasedLogSampler implements LogSampler {

  private final TimeProvider timeProvider;
  private final AtomicLong nextTraceLogTime = new AtomicLong();
  private final AtomicLong nextDebugLogTime = new AtomicLong();
  private final AtomicLong nextInfoLogTime = new AtomicLong();
  private final AtomicLong nextWarnLogTime = new AtomicLong();
  private final AtomicLong nextErrorLogTime = new AtomicLong();

  /**
   * Constructor.
   *
   * @param timeProvider the {@link TimeProvider} to provide the current time in milliseconds.
   */
  protected TimeBasedLogSampler(TimeProvider timeProvider) {
    this.timeProvider = timeProvider;
  }

  @Override
  public boolean accept(String message, int logLevel) {
    long currentTime = timeProvider.currentTimeMillis();
    switch (logLevel) {
      case LocationAwareLogger.TRACE_INT:
        return accept(message, logLevel, currentTime, nextTraceLogTime);
      case LocationAwareLogger.DEBUG_INT:
        return accept(message, logLevel, currentTime, nextDebugLogTime);
      case LocationAwareLogger.INFO_INT:
        return accept(message, logLevel, currentTime, nextInfoLogTime);
      case LocationAwareLogger.WARN_INT:
        return accept(message, logLevel, currentTime, nextWarnLogTime);
      case LocationAwareLogger.ERROR_INT:
        return accept(message, logLevel, currentTime, nextErrorLogTime);
      default:
        return true;
    }
  }

  /**
   * Computes the time which it will accept log again.
   *
   * @param message the log message being accepted
   * @param logLevel the log level of the message being accepted
   * @param currentLogTime timestamp in milliseconds that the log was accepted
   */
  protected abstract long computeNextLogTime(String message, int logLevel, long currentLogTime);

  private boolean accept(String message, int logLevel, long currentTime, AtomicLong nextLogTime) {
    long time = nextLogTime.get();
    // Check whether it is time to log.
    while (currentTime >= time) {
      // If current time is >= log time, try to determine and set the next log time
      //
      // If the current thread is able to set the next log time, then the message received by this
      // thread should be accepted
      // If the current thread is not able to set the next log time, meaning it has been updated
      // by other thread. We need to get the latest next log time to see if we still accept the message
      if (nextLogTime.compareAndSet(time, computeNextLogTime(message, logLevel, currentTime))) {
        return true;
      }
      time = nextLogTime.get();
    }
    return false;
  }
}
