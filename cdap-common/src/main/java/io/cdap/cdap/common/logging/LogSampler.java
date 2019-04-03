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

import org.slf4j.Logger;
import org.slf4j.spi.LocationAwareLogger;

/**
 * This interface defines sampling on log events. It is mainly used in conjunction with the
 * {@link Loggers#sampling(Logger, LogSampler)} method.
 * Common implementations can be found in {@link LogSamplers}.
 */
public interface LogSampler {

  /**
   * Returns the decision of whether to accept the log message of the given log level.
   *
   * @param message the log message before formatting has been applies
   * @param logLevel the log level. The value are declared in {@link LocationAwareLogger}
   * @return {@code true} to accept or {@code false} to decline
   */
  boolean accept(String message, int logLevel);
}
