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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Reconfigurable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handy implementation of {@link DatasetDefinition} that implements basic methods by delegating logic execution to
 * underlying dataset definitions.
 *
 * @param <D> defines data operations that can be performed on this dataset instance
 */
@Beta
public abstract class CompositeDatasetDefinition<D extends Dataset>
  extends AbstractDatasetDefinition<D, DatasetAdmin> implements Reconfigurable {

  private final Map<String, DatasetDefinition> delegates;

  /**
   * Constructor that takes an info about underlying datasets
   * @param name this dataset type name
   * @param delegates map of [dataset instance name] -> [dataset definition] to use for this instance name
   */
  protected CompositeDatasetDefinition(String name, Map<String, ? extends DatasetDefinition> delegates) {
    super(name);
    this.delegates = new HashMap<>(delegates);
    for (Map.Entry<String, ? extends DatasetDefinition> entry : delegates.entrySet()) {
      if (null == entry.getValue()) {
        throw new IllegalArgumentException(String.format("Dataset definition for '%s' is required", entry.getKey()));
      }
    }
  }

  /**
   * Constructor that takes one underlying dataset.
   * @param name this dataset type name
   * @param delegateName name of the first delegate
   * @param delegate dataset definition for the second delegate
   */
  protected CompositeDatasetDefinition(String name,
                                       String delegateName, DatasetDefinition delegate) {
    this(name, Collections.singletonMap(delegateName, delegate));
  }

  /**
   * Constructor that takes two underlying datasets.
   * @param name this dataset type name
   * @param delegateNameA name of the first delegate
   * @param delegateA dataset definition for the first delegate
   * @param delegateNameB name of the second delegate
   * @param delegateB dataset definition for the second delegate
   */
  protected CompositeDatasetDefinition(String name,
                                       String delegateNameA, DatasetDefinition delegateA,
                                       String delegateNameB, DatasetDefinition delegateB) {
    this(name, makeMap(delegateNameA, delegateA, delegateNameB, delegateB));
  }

  private static Map<String, ? extends DatasetDefinition> makeMap(String delegateNameA, DatasetDefinition delegateA,
                                                                  String delegateNameB, DatasetDefinition delegateB) {
    Map<String, DatasetDefinition> map = new HashMap<>();
    map.put(delegateNameA, delegateA);
    map.put(delegateNameB, delegateB);
    return map;
  }

  /**
   * Gets a {@link Dataset} instance from the delegates with the given instance name.
   *
   * @param datasetContext the context which the get call is happening
   * @param name name of the dataset
   * @param spec the {@link DatasetSpecification} for the outer dataset
   * @param arguments dataset arguments
   * @param classLoader classloader for loading the dataset
   * @param <T> type of the dataset
   * @return a dataset instance
   * @throws IOException if failed to get a dataset instance
   */
  @SuppressWarnings("unchecked")
  protected final <T extends Dataset> T getDataset(DatasetContext datasetContext, String name,
                                                   DatasetSpecification spec, Map<String, String> arguments,
                                                   ClassLoader classLoader) throws IOException {

    // NOTE: by default we propagate properties to the embedded datasets
    return (T) delegates.get(name).getDataset(datasetContext, spec.getSpecification(name), arguments, classLoader);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    List<DatasetSpecification> specs = this.delegates.entrySet().stream()
      .map(impl -> impl.getValue().configure(impl.getKey(), properties))
      .collect(Collectors.toList());
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(specs)
      .build();
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
                                          DatasetProperties newProperties,
                                          DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    List<DatasetSpecification> specs = new ArrayList<>();
    for (Map.Entry<String, ? extends DatasetDefinition> impl : this.delegates.entrySet()) {
      specs.add(reconfigure(impl.getValue(), impl.getKey(),
                            newProperties, currentSpec.getSpecification(impl.getKey())));
    }
    return DatasetSpecification.builder(instanceName, getName())
      .properties(newProperties.getProperties())
      .datasets(specs)
      .build();
  }

  @Override
  public final DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                     ClassLoader classLoader) throws IOException {
    if (1 == delegates.size()) {
      // for a single delegate, we don't need a composite admin
      return getAdmin(datasetContext, delegates.keySet().iterator().next(), spec, classLoader);
    }
    Map<String, DatasetAdmin> admins = new HashMap<>();
    for (String name : this.delegates.keySet()) {
      admins.put(name, getAdmin(datasetContext, name, spec, classLoader));
    }
    return new CompositeDatasetAdmin(admins);
  }

  private DatasetAdmin getAdmin(DatasetContext datasetContext, String name,
                                DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return delegates.get(name).getAdmin(datasetContext, spec.getSpecification(name), classLoader);
  }

  protected <DS extends Dataset, DA extends DatasetAdmin> DatasetDefinition<DS, DA> getDelegate(String name) {
    @SuppressWarnings("unchecked")
    DatasetDefinition<DS, DA> def = delegates.get(name);
    return def;
  }
}

