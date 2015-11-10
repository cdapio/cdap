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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class provides the specification for a MapReduce job.
 */
public class MapReduceSpecification implements ProgramSpecification, PropertyProvider {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;
  private final Map<String, String> properties;
  private final String inputDataSet;
  private final String outputDataSet;
  private final Resources driverResources;
  private final Resources mapperResources;
  private final Resources reducerResources;

  public MapReduceSpecification(String className, String name, String description, String inputDataSet,
                                String outputDataSet, Set<String> dataSets, Map<String, String> properties,
                                @Nullable Resources driverResources,
                                @Nullable Resources mapperResources, @Nullable Resources reducerResources) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.inputDataSet = inputDataSet;
    this.outputDataSet = outputDataSet;
    this.properties = Collections.unmodifiableMap(properties == null ? Collections.<String, String>emptyMap()
                                                    : new HashMap<>(properties));
    this.driverResources = driverResources;
    this.mapperResources = mapperResources;
    this.reducerResources = reducerResources;
    this.dataSets = getAllDatasets(dataSets, inputDataSet, outputDataSet);
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
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return An immutable set of {@link co.cask.cdap.api.dataset.Dataset DataSets} that
   *         are used by the {@link MapReduce}.
   */
  public Set<String> getDataSets() {
    return dataSets;
  }

  /**
   * @return name of the dataset to be used as output of mapreduce job or {@code null} if no dataset is used as output
   *         destination
   */
  @Nullable
  public String getOutputDataSet() {
    return outputDataSet;
  }

  /**
   * @return name The name of the dataset to be used as input to a MapReduce job or {@code null}
   * if no dataset is used as the input source.
   */
  @Nullable
  public String getInputDataSet() {
    return inputDataSet;
  }

  /**
   * @return Resources requirement for driver or {@code null} if not specified.
   */
  @Nullable
  public Resources getDriverResources() {
    return driverResources;
  }

  /**
   * @return Resources requirement for mapper task or {@code null} if not specified.
   */
  @Nullable
  public Resources getMapperResources() {
    return mapperResources;
  }

  /**
   * @return Resources requirement for reducer task or {@code null} if not specified.
   */
  @Nullable
  public Resources getReducerResources() {
    return reducerResources;
  }

  private Set<String> getAllDatasets(Set<String> dataSets, String inputDataSet, String outputDataSet) {
    Set<String> allDatasets = new HashSet<>(dataSets);

    if (inputDataSet != null && !inputDataSet.isEmpty()) {
      allDatasets.add(inputDataSet);
    }

    if (outputDataSet != null && !outputDataSet.isEmpty()) {
      allDatasets.add(outputDataSet);
    }

    return Collections.unmodifiableSet(allDatasets);
  }
}
