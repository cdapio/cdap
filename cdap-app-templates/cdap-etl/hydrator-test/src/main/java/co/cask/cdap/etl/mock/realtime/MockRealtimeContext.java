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

package co.cask.cdap.etl.mock.realtime;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.mock.common.NoopMetrics;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock RealtimeContext for tests.
 */
public class MockRealtimeContext implements RealtimeContext {
  private final PluginProperties pluginProperties;

  public MockRealtimeContext(Map<String, String> properties) {
    this.pluginProperties = PluginProperties.builder().addAll(properties).build();
  }

  public MockRealtimeContext() {
    this(Maps.<String, String>newHashMap());
  }

  @Override
  public PluginProperties getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public StageMetrics getMetrics() {
    return NoopMetrics.INSTANCE;
  }

  @Override
  public String getStageName() {
    return "singleStage";
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getInstanceCount() {
    return 1;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Nullable
  @Override
  public Schema getInputSchema() {
    return null;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public Schema getOutputSchema() {
    return null;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return null;
  }
}

