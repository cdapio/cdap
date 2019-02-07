/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.nosql.dataset;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;

import java.io.IOException;
import java.util.Map;

/**
 * Dataset context for entity tables. The regular dataset context classes cannot be used due to cyclic dependency
 * between dataset service and NoSQL StructuredTable.
 */
class EntityTableDatasetContext implements DatasetContext, AutoCloseable {
  private final TransactionContext txContext;
  private final TableDatasetSupplier datasetAccesor;
  private Dataset entityTable = null;

  EntityTableDatasetContext(TransactionContext txContext, TableDatasetSupplier datasetAccesor) {
    this.txContext = txContext;
    this.datasetAccesor = datasetAccesor;
  }

  @Override
  public void close() throws Exception {
    if (entityTable != null) {
      entityTable.close();
      entityTable = null;
    }
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments) {
    // this is the only method that gets called on this dataset context (see NoSqlStructuredTableContext)
    if (entityTable == null) {
      try {
        entityTable = datasetAccesor.getTableDataset(name, arguments);
        txContext.addTransactionAware((TransactionAware) entityTable);
      } catch (IOException e) {
        throw new DatasetInstantiationException("Cannot instantiate entity table", e);
      }
    }
    //noinspection unchecked
    return (T) entityTable;
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    // no-op
  }

  @Override
  public void discardDataset(Dataset dataset) {
    // no-op
  }
}
