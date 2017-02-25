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

import org.slf4j.spi.LocationAwareLogger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract {@link LogSampler} implementation that sample based on number of log events.
 * It maintains a different call count for each log level.
 */
public abstract class CountBasedLogSampler implements LogSampler {

  private final AtomicLong traces = new AtomicLong();
  private final AtomicLong debugs = new AtomicLong();
  private final AtomicLong infos = new AtomicLong();
  private final AtomicLong warns = new AtomicLong();
  private final AtomicLong errors = new AtomicLong();

  @Override
  public boolean accept(String message, int logLevel) {
    switch (logLevel) {
      case LocationAwareLogger.TRACE_INT:
        return accept(message, logLevel, traces.incrementAndGet());

      case LocationAwareLogger.DEBUG_INT:
        return accept(message, logLevel, debugs.incrementAndGet());

      case LocationAwareLogger.INFO_INT:
        return accept(message, logLevel, infos.incrementAndGet());

      case LocationAwareLogger.WARN_INT:
        return accept(message, logLevel, warns.incrementAndGet());

      case LocationAwareLogger.ERROR_INT:
        return accept(message, logLevel, errors.incrementAndGet());

      default:
        // This shouldn't happen
        return true;
    }
  }

  /**
   * Returns the decision whether to accept the log message.
   *
   * @param message the log message
   * @param logLevel the log level of the log event
   * @param callCount number of times the log at this level has been called
   */
  protected abstract boolean accept(String message, int logLevel, long callCount);
}
