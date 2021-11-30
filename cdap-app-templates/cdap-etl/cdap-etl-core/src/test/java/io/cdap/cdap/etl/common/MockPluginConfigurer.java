/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Mock {@link PluginConfigurer} for unit tests. No-op for dataset methods.
 * Use the setter methods to populate the plugin objects that should be returned by the PluginConfigurer methods.
 */
public class MockPluginConfigurer implements PluginConfigurer, DatasetConfigurer, FeatureFlagsProvider {
  private Map<Key, Value> plugins;

  private static class Value {
    private Object plugin;
    private SortedMap<ArtifactId, PluginClass> artifacts;

    public Value(Object plugin, Set<ArtifactId> artifacts) {
      this.plugin = plugin;
      this.artifacts = new TreeMap<>();
      for (ArtifactId artifactId : artifacts) {
        this.artifacts.put(artifactId, PluginClass.builder().setName("name").setType("type").setDescription("desc")
                                         .setClassName("clsname").setConfigFieldName("cfgfield")
                                         .setProperties(ImmutableMap.of()).build());
      }
    }
  }

  public MockPluginConfigurer() {
    plugins = new HashMap<>();
  }

  public void addMockPlugin(String type, String name, Object plugin, Set<ArtifactId> artifacts) {
    Key key = new Key(type, name);
    Value value = new Value(plugin, artifacts);
    plugins.put(key, value);
  }

  @Override
  public Map<String, String> getFeatureFlags() {
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
                         PluginProperties properties, PluginSelector selector) {
    Value value = plugins.get(new Key(pluginType, pluginName));
    if (value == null) {
      return null;
    }
    selector.select(value.artifacts);
    return (T) value.plugin;
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties, PluginSelector selector) {
    return null;
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    MacroParser macroParser = new MacroParser(evaluator, options);
    Map<String, String> evaluated = new HashMap<>();
    properties.forEach((key, val) -> evaluated.put(key, macroParser.parse(val)));
    return evaluated;
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {

  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {

  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {

  }

  @Override
  public void createDataset(String datasetName, String typeName) {

  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {

  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {

  }

  private static class Key {
    private final String type;
    private final String name;

    public Key(String type, String name) {
      this.type = type;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key that = (Key) o;

      return Objects.equals(type, that.type) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }
  }
}
