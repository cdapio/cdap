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
 * An ApplicationTemplate is a special type of {@link Application} that can be given a configuration object to
 * create an application instance. An instance of an ApplicationTemplate is called an Adapter. One template can
 * be used to create multiple Adapters. Currently, an ApplicationTemplate must have exactly one Workflow, or
 * exactly on Worker.
 *
 * @param <T> type of the configuration object
 */
@Beta
public abstract class ApplicationTemplate<T> implements Application {

  /**
   * Called when an adapter is created in order to define what Datasets, Streams, Plugins, and runtime arguments
   * should be available to the adapter, as determined by the given configuration.
   *
   * @param name name of the adapter
   * @param configuration adapter configuration. It will be {@code null} if there is no configuration provided.
   * @param configurer {@link AdapterConfigurer} used to configure the adapter.
   * @throws IllegalArgumentException if the configuration is not valid
   * @throws Exception if there was some other error configuring the adapter
   */
  public void configureAdapter(String name, @Nullable T configuration, AdapterConfigurer configurer) throws Exception {
    // no-op
  }
}
