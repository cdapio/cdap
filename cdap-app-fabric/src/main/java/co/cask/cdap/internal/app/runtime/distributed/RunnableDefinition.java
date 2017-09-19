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

package co.cask.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillRunnable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Container class for holding {@link TwillRunnable} and extra configurations associated with it.
 */
final class RunnableDefinition {
  private final TwillRunnable runnable;
  private final ResourceSpecification resources;
  private final Map<String, String> twillRunnableConfigs;
  private final Map<String, Level> logLevels;
  private final Integer maxRetries;

  RunnableDefinition(TwillRunnable runnable, ResourceSpecification resources,
                     Map<String, String> twillRunnableConfigs, Map<String, Level> logLevels,
                     @Nullable Integer maxRetries) {
    this.runnable = runnable;
    this.resources = resources;
    this.twillRunnableConfigs = twillRunnableConfigs;
    this.logLevels = logLevels;
    this.maxRetries = maxRetries;
  }

  TwillRunnable getRunnable() {
    return runnable;
  }

  ResourceSpecification getResources() {
    return resources;
  }

  @Nullable
  Integer getMaxRetries() {
    return maxRetries;
  }

  Map<String, String> getTwillRunnableConfigs() {
    return twillRunnableConfigs;
  }

  Map<String, Level> getLogLevels() {
    return logLevels;
  }
}
