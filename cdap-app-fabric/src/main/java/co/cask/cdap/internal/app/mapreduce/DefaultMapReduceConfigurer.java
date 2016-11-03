/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import co.cask.cdap.proto.Id;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link MapReduceConfigurer}.
 */
public final class DefaultMapReduceConfigurer extends DefaultPluginConfigurer implements MapReduceConfigurer {

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
                                    ArtifactRepository artifactRepository,
                                    PluginInstantiator pluginInstantiator) {
    super(deployNamespace, artifactId, artifactRepository, pluginInstantiator);
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
                                      properties, driverResources, mapperResources, reducerResources);
  }
}
