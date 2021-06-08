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

package io.cdap.cdap.datapipeline.connection;

import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;

/**
 * Default implementation of connector context
 */
public class DefaultConnectorContext implements ConnectorContext {
  private final FailureCollector collector;
  private final PluginConfigurer pluginConfigurer;

  public DefaultConnectorContext(FailureCollector collector, PluginConfigurer pluginConfigurer) {
    this.collector = collector;
    this.pluginConfigurer = pluginConfigurer;
  }

  @Override
  public FailureCollector getFailureCollector() {
    return collector;
  }

  @Override
  public PluginConfigurer getPluginConfigurer() {
    return pluginConfigurer;
  }
}
