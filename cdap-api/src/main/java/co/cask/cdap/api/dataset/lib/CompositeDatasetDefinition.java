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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handy implementation of {@link DatasetDefinition} that implements basic methods by delegating logic execution to
 * underlying dataset definitions.
 *
 * @param <D> defines data operations that can be performed on this dataset instance
 */
@Beta
public abstract class CompositeDatasetDefinition<D extends Dataset>
  extends AbstractDatasetDefinition<D, DatasetAdmin> {

  private final Map<String, ? extends DatasetDefinition> delegates;

  /**
   * Constructor that takes an info about underlying datasets
   * @param name this dataset type name
   * @param delegates map of [dataset instance name] -> [dataset definition] to use for this instance name
   */
  protected CompositeDatasetDefinition(String name, Map<String, ? extends DatasetDefinition> delegates) {
    super(name);
    this.delegates = delegates;
  }

  /**
   * Gets dataset for underlying dataset instance of given name
   * @param name dataset instance name
   * @param type expected type of the dataset
   * @param spec parent dataset instance name
   * @param <T> expected type of the dataset
   * @return dataset to perform data operations
   * @throws IOException
   */
  protected final <T extends Dataset> T getDataset(DatasetContext datasetContext, String name, Class<T> type,
                                                   DatasetSpecification spec, Map<String, String> arguments,
                                                   ClassLoader classLoader) throws IOException {

    return (T) delegates.get(name).getDataset(datasetContext, spec.getSpecification(name), arguments, classLoader);
  }

  protected final <T extends Dataset> T getDataset(DatasetContext datasetContext, String name,
                                                   DatasetSpecification spec, Map<String, String> arguments,
                                                   ClassLoader classLoader) throws IOException {

    // NOTE: by default we propagate properties to the embedded datasets
    return (T) delegates.get(name).getDataset(datasetContext, spec.getSpecification(name), arguments, classLoader);
  }

  @Override
  public final DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    List<DatasetSpecification> specs = Lists.newArrayList();
    for (Map.Entry<String, ? extends DatasetDefinition> impl : this.delegates.entrySet()) {
      specs.add(impl.getValue().configure(impl.getKey(), properties));
    }

    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(specs)
      .build();
  }

  @Override
  public final DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                     ClassLoader classLoader) throws IOException {
    List<DatasetAdmin> admins = Lists.newArrayList();
    for (Map.Entry<String, ? extends DatasetDefinition> impl : this.delegates.entrySet()) {
      admins.add(impl.getValue().getAdmin(datasetContext, spec.getSpecification(impl.getKey()), classLoader));
    }

    return new CompositeDatasetAdmin(admins);
  }
}

