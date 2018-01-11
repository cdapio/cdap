/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.plugin.PluginSelector;
import com.google.common.collect.ImmutableMap;

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
public class MockPluginConfigurer implements PluginConfigurer, DatasetConfigurer {
  private Map<Key, Value> plugins;

  private static class Value {
    private Object plugin;
    private SortedMap<ArtifactId, PluginClass> artifacts;

    public Value(Object plugin, Set<ArtifactId> artifacts) {
      this.plugin = plugin;
      this.artifacts = new TreeMap<>();
      for (ArtifactId artifactId : artifacts) {
        this.artifacts.put(artifactId, new PluginClass("type", "name", "desc", "clsname", "cfgfield",
                                                       ImmutableMap.<String, PluginPropertyField>of()));
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
  public void addStream(Stream stream) {

  }

  @Override
  public void addStream(String streamName) {

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
