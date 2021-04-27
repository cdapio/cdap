/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.dispatcher;

import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.internal.app.deploy.DefaultConfigResponse;

import javax.annotation.Nullable;

/**
 * This class is used by {@link io.cdap.cdap.internal.app.dispatcher.ConfiguratorTask} to communicate the result of the
 * task.
 */
public final class ConfigResponseResult {
  private final DefaultConfigResponse configResponse;
  private final Exception exception;

  public ConfigResponseResult(@Nullable ConfigResponse configResponse, @Nullable Exception exception) {
    this.configResponse = (DefaultConfigResponse) configResponse;
    this.exception = exception;
  }

  @Nullable
  public Exception getException() {
    return exception;
  }

  @Nullable
  public DefaultConfigResponse getConfigResponse() {
    return configResponse;
  }
}
