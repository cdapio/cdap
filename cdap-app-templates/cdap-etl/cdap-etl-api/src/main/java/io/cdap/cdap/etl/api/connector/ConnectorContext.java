/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Context for a connector used in each method
 */
public interface ConnectorContext {

  /**
   * Returns a failure collector.
   *
   * @return a failure collector
   */
  FailureCollector getFailureCollector();

  /**
   * Returns the plugin configurer. This is useful when the connector is not able to determine which
   * plugin to use during configure time. For example, file related connectors do not know which
   * format plugin to use without the path
   *
   * @return the plugin configurer
   */
  PluginConfigurer getPluginConfigurer();
}
