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
package co.cask.cdap.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.preview.PreviewLogger;

/**
 * Interface used by CDAP applications to log the data useful for debugging during runtime.
 *
 * WARN: This is an experimental API and will change in future.
 */
@Beta
public interface Debugger {

  /**
   * Returns {@code true} if application is running in preview mode otherwise false is returned.
   *
   * WARN: This is an experimental API and will change in future.
   */
  @Beta
  boolean isPreviewEnabled();

  /**
   * Get the {@link PreviewLogger} used to log the data.
   * @param loggerName the name of the logger with which the log data to be associated
   * @return the instance of the PreviewLogger
   *
   * WARN: This is an experimental API and will change in future.
   */
  @Beta
  PreviewLogger getPreviewLogger(String loggerName);
}
