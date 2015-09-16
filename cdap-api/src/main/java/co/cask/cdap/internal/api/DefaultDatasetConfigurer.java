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

package co.cask.cdap.internal.api;

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link DatasetConfigurer} for adding datasets and streams in Configurers.
 */
// TODO: Move this class to cdap-app-fabric once CDAP_2943 is fixed
public class DefaultDatasetConfigurer implements DatasetConfigurer {

  private final Map<String, StreamSpecification> streams = new HashMap<>();
  private final Map<String, DatasetCreationSpec> datasetSpecs = new HashMap<>();
  private final Map<String, String> datasetModules = new HashMap<>();

  public Map<String, StreamSpecification> getStreams() {
    return streams;
  }

  public Map<String, DatasetCreationSpec> getDatasetSpecs() {
    return datasetSpecs;
  }

  public Map<String, String> getDatasetModules() {
    return datasetModules;
  }

  public void addStreams(Map<String, StreamSpecification> streams) {
    Set<String> duplicateStreams = findDuplicates(streams.keySet(), this.streams.keySet());
    checkArgument(duplicateStreams.isEmpty(),
                  "Streams %s have been added already. Remove the duplicates.", duplicateStreams);
    this.streams.putAll(streams);
  }

  public void addDatasetSpecs(Map<String, DatasetCreationSpec> datasetSpecs) {
    Set<String> duplicateDatasetSpecs = findDuplicates(datasetSpecs.keySet(), this.datasetSpecs.keySet());
    checkArgument(duplicateDatasetSpecs.isEmpty(),
                  "Dataset Instances %s have already been added. Remove the duplicates.", duplicateDatasetSpecs);
    this.datasetSpecs.putAll(datasetSpecs);
  }

  public void addDatasetModules(Map<String, String> datasetModules) {
    for (Map.Entry<String, String> newEntry : datasetModules.entrySet()) {
      if (datasetModules.containsKey(newEntry.getKey())) {
        Preconditions.checkArgument(datasetModules.get(newEntry.getKey()).equals(newEntry.getValue()),
                                    String.format(
                                      "DatasetModule %s has been already added with different class %s",
                                      newEntry.getKey(), datasetModules.get(newEntry.getKey())));
      }
    }
    this.datasetModules.putAll(datasetModules);
  }

  private Set<String> findDuplicates(Set<String> setOne, Set<String> setTwo) {
    Set<String> duplicates = new HashSet<>(setOne);
    duplicates.retainAll(setTwo);
    return duplicates;
  }

  @Override
  public void addStream(Stream stream) {
    checkArgument(stream != null, "Stream cannot be null.");
    StreamSpecification spec = stream.configure();
    checkArgument(!streams.containsKey(spec.getName()),
                  "Stream %s has already been added. Remove the duplicate addition.", spec.getName());
    streams.put(spec.getName(), spec);
  }

  @Override
  public void addStream(String streamName) {
    checkArgument(streamName != null && !streamName.isEmpty(), "Stream Name cannot be null or empty");
    addStream(new Stream(streamName));
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    checkArgument(moduleName != null, "Dataset module name cannot be null.");
    checkArgument(moduleClass != null, "Dataset module class cannot be null.");
    checkArgument(!datasetModules.containsKey(moduleName),
                  "DatasetModule %s has already been added. Remove the duplicate addition.", moduleName);
    datasetModules.put(moduleName, moduleClass.getName());
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    checkArgument(datasetClass != null, "Dataset class cannot be null.");
    checkArgument(!datasetModules.containsKey(datasetClass.getName()),
                  "DatasetType %s has already been added. Remove the duplicate addition.", datasetClass.getName());
    datasetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public void createDataset(String datasetInstanceName, String typeName, DatasetProperties properties) {
    checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    checkArgument(typeName != null, "Dataset type name cannot be null.");
    checkArgument(properties != null, "Instance properties name cannot be null.");
    checkArgument(!datasetSpecs.containsKey(datasetInstanceName),
                  "DatasetInstance %s has already been added. Remove the duplicate addition.", datasetInstanceName);
    datasetSpecs.put(datasetInstanceName,
                         new DatasetCreationSpec(datasetInstanceName, typeName, properties));
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  @Override
  public void createDataset(String datasetInstanceName, Class<? extends Dataset> datasetClass,
                            DatasetProperties properties) {
    checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    checkArgument(datasetClass != null, "Dataset class name cannot be null.");
    checkArgument(properties != null, "Instance properties name cannot be null.");
    checkArgument(!datasetSpecs.containsKey(datasetInstanceName),
                  "DatasetInstance %s has already been added. Remove the duplicate addition.", datasetInstanceName);
    datasetSpecs.put(datasetInstanceName,
                     new DatasetCreationSpec(datasetInstanceName, datasetClass.getName(), properties));
    datasetModules.put(datasetClass.getName(), datasetClass.getName());
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    createDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }

  private void checkArgument(boolean condition, String template, Object...args) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(template, args));
    }
  }
}
