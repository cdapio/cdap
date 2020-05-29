/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.api.app;

import io.cdap.cdap.api.Config;

/**
 * Stores results of upgrading an application config.
 *
 * @param <T> {@link Config} config class that represents the configuration type of an Application.
 */
public class ApplicationUpdateResult<T extends Config> {

  // Upgraded config.
  private final T newConfig;

  public ApplicationUpdateResult(T newConfig) {
    this.newConfig = newConfig;
  }

  public T getNewConfig() {
    return newConfig;
  }
}
