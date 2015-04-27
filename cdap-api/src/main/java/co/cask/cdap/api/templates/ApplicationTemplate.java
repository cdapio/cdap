/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.Application;

import javax.annotation.Nullable;

/**
 * Abstract App Template class that provides additional functionality required for App Templates.
 *
 * @param <T> type of the configuration object
 */
//TODO: Add more description about what an app template is.
@Beta
public abstract class ApplicationTemplate<T> implements Application {

  /**
   * Given the adapter configuration, configures the {@link AdapterConfigurer}.
   *
   * @param name name of the adapter
   * @param configuration adapter configuration. It will be {@code null} if there is no configuration provided.
   * @param configurer {@link AdapterConfigurer}
   * @throws IllegalArgumentException if the configuration is not valid
   * @throws Exception if there was some other error configuring the adapter
   */
  public void configureAdapter(String name, @Nullable T configuration, AdapterConfigurer configurer) throws Exception {
    // no-op
  }
}
