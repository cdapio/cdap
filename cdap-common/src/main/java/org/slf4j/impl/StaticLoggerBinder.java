/*
 * Copyright © 2026 Cask Data, Inc.
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

package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

/**
 * Compatibility shim for SLF4J 1.7 {@code StaticLoggerBinder} callers (such as Spark 3.1/3.3
 * {@code Logging.isLog4j2()}) when running on SLF4J 2.x and Logback 1.6.x, which bind via
 * {@link org.slf4j.spi.SLF4JServiceProvider} instead of {@code org.slf4j.impl.StaticLoggerBinder}.
 */
// LoggerFactoryBinder is deprecated in SLF4J 2.x; implemented intentionally as an SLF4J 1.7 shim.
@SuppressWarnings("deprecation")
public final class StaticLoggerBinder implements LoggerFactoryBinder {

  /**
   * Legacy SLF4J 1.7 API compatibility version exposed for callers inspecting {@code StaticLoggerBinder}.
   */
  public static final String REQUESTED_API_VERSION = "1.7.16";

  private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();

  private StaticLoggerBinder() {
  }

  /**
   * Returns the singleton {@link StaticLoggerBinder} instance.
   */
  public static StaticLoggerBinder getSingleton() {
    return SINGLETON;
  }

  @Override
  public ILoggerFactory getLoggerFactory() {
    return LoggerFactory.getILoggerFactory();
  }

  @Override
  public String getLoggerFactoryClassStr() {
    return LoggerFactory.getILoggerFactory().getClass().getName();
  }
}
