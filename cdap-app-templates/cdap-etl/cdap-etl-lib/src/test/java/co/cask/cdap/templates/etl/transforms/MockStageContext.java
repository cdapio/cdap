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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.common.NoopMetrics;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Mock context for unit tests
 */
public class MockStageContext implements StageContext {
  private final PluginProperties pluginProperties;

  public MockStageContext(Map<String, String> args) {
    this.pluginProperties = PluginProperties.builder().addAll(args).build();
  }

  public MockStageContext() {
    this(Maps.<String, String>newHashMap());
  }

  @Override
  public PluginProperties getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public Metrics getMetrics() {
    return NoopMetrics.INSTANCE;
  }
}
