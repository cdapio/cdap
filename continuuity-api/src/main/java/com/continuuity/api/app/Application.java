/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.app;

/**
 * Defines a Continuuity Reactor Application.
 *
 */
public interface Application {
  /**
   * Configures the Application.
   *
   * @param configurer Collects the Application configuration
   * @param context Used to access the environment, application configuration, and application (deployment) arguments
   */
  void configure(ApplicationConfigurer configurer, ApplicationContext context);
}
