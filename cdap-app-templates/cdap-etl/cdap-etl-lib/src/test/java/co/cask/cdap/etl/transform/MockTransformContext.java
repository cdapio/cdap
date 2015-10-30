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

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.MockMetrics;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Mock context for unit tests
 */
public class MockTransformContext implements TransformContext {
  private final PluginProperties pluginProperties;
  private final Metrics metrics;

  public MockTransformContext(Map<String, String> args) {
    this(args, new MockMetrics());
  }

  public MockTransformContext(Map<String, String> args, Metrics metrics) {
    this.pluginProperties = PluginProperties.builder().addAll(args).build();
    this.metrics = metrics;
  }

  public MockTransformContext() {
    this(Maps.<String, String>newHashMap());
  }

  @Override
  public PluginProperties getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public int getStageId() {
    return 1;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }
}
