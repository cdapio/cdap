/*
 * Copyright © 2014 Cask Data, Inc.
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
 * Defines a CDAP Application.
 *
 * @param <T> {@link Config} class that represents the configuration of the Application.
 */
public interface Application<T extends Config> {

  /**
   * Configures the Application.
   *
   * @param configurer Collects the Application configuration
   * @param context Used to access the environment, application configuration, and application
   *     (deployment) arguments
   */
  void configure(ApplicationConfigurer configurer, ApplicationContext<T> context);

  /**
   * Returns if application supports config update or not.
   */
  default boolean isUpdateSupported() {
    return false;
  }

  /**
   * Updates application configuration based on config and update actions inside
   * applicationUpdateContext.
   *
   * @param applicationUpdateContext Used to access methods helpful for operations like
   *     upgrading plugin version for config.
   * @return {@link ApplicationUpdateResult} object for the config update operation.
   * @throws UnsupportedOperationException if application does not support config update
   *     operation.
   * @throws Exception if there was an exception during update of app config. This exception
   *     will often wrap the actual exception.
   */
  default ApplicationUpdateResult<T> updateConfig(ApplicationUpdateContext applicationUpdateContext)
      throws Exception {
    throw new UnsupportedOperationException(
        "Application config update operation is not supported.");
  }

  default ApplicationValidationResult validateConfig(ApplicationValidationContext applicationValidationContext)
    throws Exception {
    throw new UnsupportedOperationException(
        "Application config validation operation is not supported");
  }
}
