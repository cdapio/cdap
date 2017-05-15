/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
      if (this.datasetModules.containsKey(newEntry.getKey())) {
        checkArgument(this.datasetModules.get(newEntry.getKey()).equals(newEntry.getValue()),
                      String.format(
                        "DatasetModule %s has been already added with different class %s",
                        newEntry.getKey(), this.datasetModules.get(newEntry.getKey())));
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

    StreamSpecification existingSpec = streams.get(spec.getName());
    if (existingSpec != null && !existingSpec.equals(spec)) {
      throw new IllegalArgumentException(String.format("Stream '%s' was added multiple times with different specs. " +
        "Please resolve the conflict so that there is only one spec for the stream.", spec.getName()));
    }
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

    String moduleClassName = moduleClass.getName();
    String existingModuleClass = datasetModules.get(moduleName);
    if (existingModuleClass != null && !existingModuleClass.equals(moduleClass.getName())) {
      throw new IllegalArgumentException(String.format("Module '%s' added multiple times with different classes " +
        "'%s' and '%s'. Please resolve the conflict.", moduleName, existingModuleClass, moduleClassName));
    }
    datasetModules.put(moduleName, moduleClassName);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    checkArgument(datasetClass != null, "Dataset class cannot be null.");

    String className = datasetClass.getName();
    String existingClassName = datasetModules.get(datasetClass.getName());
    if (existingClassName != null && !existingClassName.equals(className)) {
      throw new IllegalArgumentException(String.format("Dataset class '%s' was added already as a module with class " +
        "'%s'. Please resolve the conflict so there is only one class.", className, existingClassName));
    }
    datasetModules.put(datasetClass.getName(), className);
  }

  /**
   * This adds the dataset class as a dataset type that needs to be deployed implicitly, as the results of a
   * {@link #createDataset(String, Class)} or {@link #createDataset(String, Class, DatasetProperties)} call.
   * The class name is recorded with a ".implicit." prefix, which can never collide with an actual class name.
   *
   * @param datasetClass the dataset class to add
   */
  private void addImplicitDatasetType(Class<? extends Dataset> datasetClass) {
    String className = datasetClass.getName();
    // TODO: this is a bit of a hack because the map is from String to String.
    datasetModules.put(".implicit." + className, className);
  }

  @Override
  public void createDataset(String datasetInstanceName, String typeName, DatasetProperties properties) {
    checkArgument(datasetInstanceName != null, "Dataset instance name cannot be null.");
    checkArgument(typeName != null, "Dataset type name cannot be null.");
    checkArgument(properties != null, "Instance properties name cannot be null.");

    DatasetCreationSpec spec = new DatasetCreationSpec(datasetInstanceName, typeName, properties);
    DatasetCreationSpec existingSpec = datasetSpecs.get(datasetInstanceName);
    if (existingSpec != null && !existingSpec.equals(spec)) {
      throw new IllegalArgumentException(String.format("DatasetInstance '%s' was added multiple times with " +
        "different specifications. Please resolve the conflict so that there is only one specification for " +
        "the dataset instance.", datasetInstanceName));
    }
    datasetSpecs.put(datasetInstanceName, spec);
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  @Override
  public void createDataset(String datasetInstanceName, Class<? extends Dataset> datasetClass,
                            DatasetProperties properties) {
    createDataset(datasetInstanceName, datasetClass.getName(), properties);
    addImplicitDatasetType(datasetClass);
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
