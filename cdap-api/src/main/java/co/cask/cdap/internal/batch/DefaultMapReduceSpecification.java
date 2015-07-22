/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public class DefaultMapReduceSpecification implements MapReduceSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;
  private final Map<String, String> properties;
  private final String inputDataSet;
  private final String outputDataSet;
  private final Resources mapperResources;
  private final Resources reducerResources;
  private final Map<String, StreamSpecification> streams;
  private final Map<String, String> dataSetModules;
  private final Map<String, DatasetCreationSpec> dataSetInstances;

  public DefaultMapReduceSpecification(String className, String name, String description, String inputDataSet,
                                       String outputDataSet, Set<String> dataSets, Map<String, String> properties,
                                       Resources mapperResources, Resources reducerResources,
                                       Map<String, StreamSpecification> streams, Map<String, String> dataSetModules,
                                       Map<String, DatasetCreationSpec> dataSetInstances) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.inputDataSet = inputDataSet;
    this.outputDataSet = outputDataSet;
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
    this.mapperResources = mapperResources;
    this.reducerResources = reducerResources;
    this.dataSets = getAllDatasets(dataSets, inputDataSet, outputDataSet);
    this.streams = Collections.unmodifiableMap(streams);
    this.dataSetModules = Collections.unmodifiableMap(dataSetModules);
    this.dataSetInstances = Collections.unmodifiableMap(dataSetInstances);
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Set<String> getDataSets() {
    return dataSets;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Nullable
  @Override
  public String getOutputDataSet() {
    return outputDataSet;
  }

  @Nullable
  @Override
  public String getInputDataSet() {
    return inputDataSet;
  }

  @Nullable
  @Override
  public Resources getMapperResources() {
    return mapperResources;
  }

  @Nullable
  @Override
  public Resources getReducerResources() {
    return reducerResources;
  }

  @Override
  public Map<String, StreamSpecification> getStreams() {
    return streams;
  }

  @Override
  public Map<String, String> getDatasetModules() {
    return dataSetModules;
  }

  @Override
  public Map<String, DatasetCreationSpec> getDatasetSpecs() {
    return dataSetInstances;
  }

  private Set<String> getAllDatasets(Set<String> dataSets, String inputDataSet, String outputDataSet) {
    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();
    builder.addAll(dataSets);

    if (inputDataSet != null && !inputDataSet.isEmpty()) {
      builder.add(inputDataSet);
    }

    if (outputDataSet != null && !outputDataSet.isEmpty()) {
      builder.add(outputDataSet);
    }

    return builder.build();
  }
}
