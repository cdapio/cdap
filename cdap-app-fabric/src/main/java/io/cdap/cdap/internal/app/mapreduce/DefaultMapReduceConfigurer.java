/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.mapreduce;

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceConfigurer;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.internal.specification.DataSetFieldExtractor;
import io.cdap.cdap.internal.specification.PropertyFieldExtractor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link MapReduceConfigurer}.
 */
public final class DefaultMapReduceConfigurer extends AbstractConfigurer implements MapReduceConfigurer {

  private final MapReduce mapReduce;
  private String name;
  private String description;
  private Map<String, String> properties;
  private String inputDataset;
  private String outputDataset;
  private Resources driverResources;
  private Resources mapperResources;
  private Resources reducerResources;

  public DefaultMapReduceConfigurer(MapReduce mapReduce, Id.Namespace deployNamespace, Id.Artifact artifactId,
                                    PluginFinder pluginFinder,
                                    PluginInstantiator pluginInstantiator) {
    super(deployNamespace, artifactId, pluginFinder, pluginInstantiator);
    this.mapReduce = mapReduce;
    this.name = mapReduce.getClass().getSimpleName();
    this.description = "";
    this.properties = new HashMap<>();
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
  public void setProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }

  @Override
  public void setDriverResources(Resources resources) {
    this.driverResources = resources;
  }

  @Override
  public void setMapperResources(Resources resources) {
    this.mapperResources = resources;
  }

  @Override
  public void setReducerResources(Resources resources) {
    this.reducerResources = resources;
  }

  public MapReduceSpecification createSpecification() {
    Set<String> datasets = new HashSet<>();
    Reflections.visit(mapReduce, mapReduce.getClass(), new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(datasets));
    return new MapReduceSpecification(mapReduce.getClass().getName(), name, description,
                                      inputDataset, outputDataset, datasets,
                                      properties, driverResources, mapperResources, reducerResources, getPlugins());
  }
}
