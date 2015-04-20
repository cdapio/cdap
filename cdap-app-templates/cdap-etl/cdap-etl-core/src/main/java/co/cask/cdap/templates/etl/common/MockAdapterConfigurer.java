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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.templates.AdapterConfigurer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link AdapterConfigurer} for ETL Batch Tests.
 */
//TODO: Remove/Move this class else where until we figure out how to write tests without AdapterConfigurer dependency
public class MockAdapterConfigurer implements AdapterConfigurer {
  private final Map<String, String> arguments;
  private final Set<Stream> streams;
  private final Set<Class<? extends Dataset>> datasetTypes;
  private final Map<String, Class<? extends DatasetModule>> datasetModules;
  private final Map<String, KeyValue<String, DatasetProperties>> datasetInstances;
  private Schedule schedule;
  private int instances;
  private Resources resources;

  public MockAdapterConfigurer() {
    this.arguments = Maps.newHashMap();
    this.streams = Sets.newHashSet();
    this.datasetTypes = Sets.newHashSet();
    this.datasetModules = Maps.newHashMap();
    this.datasetInstances = Maps.newHashMap();
  }

  @Override
  public void setSchedule(Schedule schedule) {
    this.schedule = schedule;
  }

  @Override
  public void setInstances(int instances) {
    this.instances = instances;
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }

  @Override
  public void addRuntimeArguments(Map<String, String> arguments) {
    this.arguments.putAll(arguments);
  }

  @Override
  public void addRuntimeArgument(String key, String value) {
    this.arguments.put(key, value);
  }

  @Override
  public void addStream(Stream stream) {
    streams.add(stream);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    datasetModules.put(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    datasetTypes.add(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    datasetInstances.put(datasetName, new KeyValue<String, DatasetProperties>(typeName, properties));
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    datasetInstances.put(datasetName, new KeyValue<String, DatasetProperties>(datasetClass.getName(), props));
  }

  public Schedule getSchedule() {
    return schedule;
  }

  public int getInstances() {
    return instances;
  }

  public Map<String, String> getArguments() {
    return ImmutableMap.copyOf(arguments);
  }

  public Resources getResources() {
    return resources;
  }

  public Set<Stream> getStreams() {
    return streams;
  }

  public Set<Class<? extends Dataset>> getDatasetTypes() {
    return datasetTypes;
  }

  public Map<String, Class<? extends DatasetModule>> getDatasetModules() {
    return datasetModules;
  }

  public Map<String, KeyValue<String, DatasetProperties>> getDatasetInstances() {
    return datasetInstances;
  }
}
