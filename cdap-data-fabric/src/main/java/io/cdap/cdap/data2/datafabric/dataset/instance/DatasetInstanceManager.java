/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.instance;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceTable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Manages dataset instances metadata
 */
public class DatasetInstanceManager {

  private final TransactionRunner transactionRunner;

  @Inject
  public DatasetInstanceManager(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Adds dataset instance metadata
   * @param namespaceId the {@link NamespaceId} to add the dataset instance to
   * @param spec {@link DatasetSpecification} of the dataset instance to be added
   */
  public void add(final NamespaceId namespaceId, final DatasetSpecification spec) {
    TransactionRunners.run(transactionRunner, context -> {
      new DatasetInstanceTable(context).write(namespaceId, spec);
    });
  }

  /**
   * @param datasetInstanceId {@link DatasetId} of the dataset instance
   * @return dataset instance's {@link DatasetSpecification}
   */
  @Nullable
  public DatasetSpecification get(final DatasetId datasetInstanceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return new DatasetInstanceTable(context).get(datasetInstanceId);
    });
  }

  /**
   * @param namespaceId {@link NamespaceId} for which dataset instances are required
   * @return collection of {@link DatasetSpecification} of all dataset instances in the given namespace
   */
  public Collection<DatasetSpecification> getAll(final NamespaceId namespaceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return new DatasetInstanceTable(context).getAll(namespaceId);
    });
  }

  /**
   * @param namespaceId {@link NamespaceId} for which dataset instances are required
   * @param properties {@link Map} of dataset properties
   * @return collection of {@link DatasetSpecification} of all dataset instances in the given namespace which
   * are having the specified properties
   */
  public Collection<DatasetSpecification> get(final NamespaceId namespaceId, final Map<String, String> properties) {
    return TransactionRunners.run(transactionRunner, context -> {
      return new DatasetInstanceTable(context).get(namespaceId, properties);
    });
  }

  /**
   * Deletes dataset instance
   * @param datasetInstanceId {@link DatasetId} of the instance to delete
   * @return true if deletion succeeded, false otherwise
   */
  public boolean delete(final DatasetId datasetInstanceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return new DatasetInstanceTable(context).delete(datasetInstanceId);
    });
  }
}
