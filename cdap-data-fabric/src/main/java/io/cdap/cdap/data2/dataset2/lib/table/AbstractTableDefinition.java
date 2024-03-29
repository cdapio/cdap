/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table;

import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.Reconfigurable;
import io.cdap.cdap.api.dataset.lib.AbstractDatasetDefinition;
import io.cdap.cdap.api.dataset.table.TableProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;

/**
 * Common configuration and reconfiguration for all Table implementations.
 *
 * @param <D> the type of the table
 * @param <A> the type of the table admin
 */
public abstract class AbstractTableDefinition<D extends Dataset, A extends DatasetAdmin>
    extends AbstractDatasetDefinition<D, A> implements Reconfigurable {

  // todo: datasets should not depend on cdap configuration!
  @Inject
  protected CConfiguration cConf;

  protected AbstractTableDefinition(String name) {
    super(name);
  }

  protected AbstractTableDefinition(String name, CConfiguration cConf) {
    super(name);
    this.cConf = cConf;
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
        .properties(properties.getProperties())
        .build();
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
      DatasetProperties properties,
      DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    validateUpdate(properties, currentSpec);
    return configure(instanceName, properties);
  }

  /**
   * Validate that the new properties for an HBase Table are compatible with its existing spec.
   *
   * @param newProperties the new properties
   * @param currentSpec the table's specification before the update
   * @throws IncompatibleUpdateException if any of the new properties is incompatible
   */
  private static void validateUpdate(DatasetProperties newProperties,
      DatasetSpecification currentSpec)
      throws IncompatibleUpdateException {

    boolean wasTransactional = DatasetsUtil.isTransactional(currentSpec.getProperties());
    boolean isTransactional = DatasetsUtil.isTransactional(newProperties.getProperties());
    if (wasTransactional != isTransactional) {
      throw new IncompatibleUpdateException(String.format(
          "Attempt to change whether the table is transactional from %s to %s.", wasTransactional,
          isTransactional));
    }
    boolean wasReadlessIncrement = TableProperties.getReadlessIncrementSupport(
        currentSpec.getProperties());
    boolean isReadlessIncrement = TableProperties.getReadlessIncrementSupport(
        newProperties.getProperties());
    if (wasReadlessIncrement && !isReadlessIncrement) {
      throw new IncompatibleUpdateException("Attempt to disable read-less increments.");
    }
    String oldColumnFamily = TableProperties.getColumnFamily(currentSpec.getProperties());
    String newColumnFamily = TableProperties.getColumnFamily(newProperties.getProperties());
    if (!oldColumnFamily.equals(newColumnFamily)) {
      throw new IncompatibleUpdateException(
          String.format("Attempt to change the column family from '%s' to '%s'.", oldColumnFamily,
              newColumnFamily));
    }
  }
}
