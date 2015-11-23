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
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.MockLookupProvider;
import co.cask.cdap.etl.common.NoopMetrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock context for unit tests
 */
public class MockTransformContext implements TransformContext {
  private final PluginProperties pluginProperties;
  private final StageMetrics metrics;
  private final LookupProvider lookup;

  public MockTransformContext() {
    this(new HashMap<String, String>());
  }

  public MockTransformContext(Map<String, String> args) {
    this(args, NoopMetrics.INSTANCE, "", new MockLookupProvider(null));
  }

  public MockTransformContext(Map<String, String> args, final Metrics metrics, final String stageMetricPrefix) {
    this(args, metrics, stageMetricPrefix, new MockLookupProvider(null));
  }

  public MockTransformContext(Map<String, String> args, final Metrics metrics, final String stageMetricPrefix,
                              LookupProvider lookup) {
    this.pluginProperties = PluginProperties.builder().addAll(args).build();
    this.lookup = lookup;
    // TODO:
    this.metrics = new StageMetrics() {
      @Override
      public void count(String metricName, int delta) {
        metrics.count(stageMetricPrefix + metricName, delta);
      }

      @Override
      public void gauge(String metricName, long value) {
        metrics.gauge(stageMetricPrefix + metricName, value);
      }

      @Override
      public void pipelineCount(String metricName, int delta) {
        metrics.count(metricName, delta);
      }

      @Override
      public void pipelineGauge(String metricName, long value) {
        metrics.gauge(metricName, value);
      }
    };
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
  public StageMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String getStageName() {
    return "singleStage";
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return lookup.provide(table, arguments);
  }
}
