/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime;

import org.apache.twill.api.logging.LogEntry;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Provides methods to update or reset log levels of a program at runtime in distributed mode.
 */
public interface LogLevelUpdater {

  /**
   * Update the log levels of the program.
   *
   * @param logLevels The {@link Map} contains the requested logger name and log level.
   * @param componentName The name of the component to update the log level. If {@code null}, all components will get
   *                      updated.
   */
  void updateLogLevels(Map<String, LogEntry.Level> logLevels, @Nullable String componentName) throws Exception;

  /**
   * Reset the log levels of the program.
   * @param loggerNames The set of logger names to be reset, if empty, all log levels will be reset.
   * @param componentName The name of the component to update the log level. If {@code null}, all components will get
   *                      reset
   */
  void resetLogLevels(Set<String> loggerNames, @Nullable String componentName) throws Exception;
}
