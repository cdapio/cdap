/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.api.app;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Beta;

/**
 * Provides access to the environment, application configuration, and application (deployment) arguments.
 *
 * @param <T> {@link Config} config class that represents the configuration of the Application.
 */
@Beta
public interface ApplicationContext<T extends Config> {

  /**
   * Get the configuration object.
   *
   * @return application configuration provided during application creation
   */
  T getConfig();

  /**
   * Returns {@code true} if application is going to run in preview mode.
   *
   * NOTE: Preview feature is experimental and will change in future.
   */
  @Beta
  boolean isPreviewEnabled();
}

