/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.mapreduce;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceConfigurer;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.internal.app.DefaultPluginConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link MapReduceConfigurer}.
 */
public final class DefaultMapReduceConfigurer extends DefaultPluginConfigurer implements MapReduceConfigurer {

  private final String className;
  private String name;
  private String description;
  private Map<String, String> properties;
  private Set<String> datasets;
  private String inputDataset;
  private String outputDataset;
  private Resources driverResources;
  private Resources mapperResources;
  private Resources reducerResources;

  public DefaultMapReduceConfigurer(MapReduce mapReduce, Id.Artifact artifactId, ArtifactRepository artifactRepository,
                                    PluginInstantiator pluginInstantiator) {
    super(artifactId, artifactRepository, pluginInstantiator);
    this.className = mapReduce.getClass().getName();
    this.name = mapReduce.getClass().getSimpleName();
    this.description = "";
    this.datasets = ImmutableSet.of();
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
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    this.datasets = ImmutableSet.copyOf(datasets);
  }

  @Override
  public void setInputDataset(String dataset) {
    this.inputDataset = dataset;
  }

  @Override
  public void setOutputDataset(String dataset) {
    this.outputDataset = dataset;
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
    return new MapReduceSpecification(className, name, description, inputDataset, outputDataset, datasets,
                                      properties, driverResources, mapperResources, reducerResources);
  }
}
