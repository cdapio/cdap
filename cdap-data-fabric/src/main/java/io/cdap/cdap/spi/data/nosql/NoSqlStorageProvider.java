/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.nosql;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StorageProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TransactionSystemClient;

/**
 * A {@link StorageProvider} implementation that uses CDAP {@link Table} dataset as the storage engine.
 */
public class NoSqlStorageProvider implements StorageProvider {

  private final NoSqlStructuredTableAdmin admin;
  private final TransactionRunner txRunner;

  @Inject
  NoSqlStorageProvider(CConfiguration cConf,
                       @Named(Constants.Dataset.TABLE_TYPE_NO_TX) DatasetDefinition registryTableDef,
                       @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition,
                       TransactionSystemClient txClient, MetricsCollectionService metricsCollectionService) {
    this.admin = new NoSqlStructuredTableAdmin(tableDefinition, new NoSqlStructuredTableRegistry(registryTableDef));
    this.txRunner = new NoSqlTransactionRunner(admin, txClient, metricsCollectionService, cConf);
  }

  @Override
  public String getName() {
    return Constants.Dataset.DATA_STORAGE_NOSQL;
  }

  @Override
  public StructuredTableAdmin getStructuredTableAdmin() {
    return admin;
  }

  @Override
  public TransactionRunner getTransactionRunner() {
    return txRunner;
  }
}
