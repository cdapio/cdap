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

package co.cask.cdap.internal.app.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.artifact.ArtifactDescriptor;
import co.cask.cdap.api.artifact.Plugin;
import co.cask.cdap.api.artifact.PluginSelector;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerConfigurer;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.internal.api.DefaultDatasetConfigurer;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.PluginNotExistsException;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link WorkerConfigurer}.
 */
public class DefaultWorkerConfigurer extends DefaultDatasetConfigurer implements WorkerConfigurer {

  private final String className;
  private final Map<String, String> propertyFields;
  private final Id.Artifact artifactId;
  private final ArtifactRepository artifactRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Map<String, Plugin> plugins = Maps.newHashMap();

  private String name;
  private String description;
  private Resources resource;
  private int instances;
  private Map<String, String> properties;
  private Set<String> datasets;

  public DefaultWorkerConfigurer(Worker worker, Id.Artifact artifactId, ArtifactRepository artifactRepository,
                                 PluginInstantiator pluginInstantiator) {
    this.name = worker.getClass().getSimpleName();
    this.className = worker.getClass().getName();
    this.propertyFields = Maps.newHashMap();
    this.description = "";
    this.resource = new Resources();
    this.instances = 1;
    this.properties = ImmutableMap.of();
    this.datasets = Sets.newHashSet();
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;

    // Grab all @Property fields
    Reflections.visit(worker, TypeToken.of(worker.getClass()), new PropertyFieldExtractor(propertyFields));
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources cannot be null.");
    this.resource = resources;
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instances must be > 0.");
    this.instances = instances;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    Iterables.addAll(this.datasets, datasets);
  }

  public WorkerSpecification createSpecification() {
    Map<String, String> properties = Maps.newHashMap(this.properties);
    properties.putAll(propertyFields);
    return new WorkerSpecification(className, name, description, properties, datasets, resource, instances, plugins
    );
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    Preconditions.checkArgument(!plugins.containsKey(pluginId),
                                "Plugin of type {}, name {} was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = null;
    try {
      pluginEntry = artifactRepository.findPlugin(artifactId, pluginType, pluginName, selector);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (PluginNotExistsException e) {
      e.printStackTrace();
    }

    if (pluginEntry == null) {
      return null;
    }
    try {
      T instance = pluginInstantiator.newInstance(pluginEntry.getKey(), pluginEntry.getValue(), properties);
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return instance;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return usePluginClass(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    Preconditions.checkArgument(!plugins.containsKey(pluginId),
                                "Plugin of type %s, name %s was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<ArtifactDescriptor, PluginClass> pluginEntry = null;
    try {
      pluginEntry = artifactRepository.findPlugin(artifactId, pluginType, pluginName, selector);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (PluginNotExistsException e) {
      e.printStackTrace();
    }

    if (pluginEntry == null) {
      return null;
    }

    // Just verify if all required properties are provided.
    // No type checking is done for now.
    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      Preconditions.checkArgument(!field.isRequired() || properties.getProperties().containsKey(field.getName()),
                                  "Required property '%s' missing for plugin of type %s, name %s.",
                                  field.getName(), pluginType, pluginName);
    }

    try {
      Class<T> cls = pluginInstantiator.loadClass(pluginEntry.getKey(), pluginEntry.getValue());
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return cls;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register the given plugin in this configurer.
   */
  private void registerPlugin(String pluginId, ArtifactDescriptor artifactDescriptor, PluginClass pluginClass,
                              PluginProperties properties) {
    plugins.put(pluginId, new Plugin(artifactDescriptor, pluginClass, properties));
  }
}
