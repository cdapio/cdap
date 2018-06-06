/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;

/**
 * Default Configuration Store.
 */
public class DefaultConfigStore implements ConfigStore {
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public DefaultConfigStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  public static void setupDatasets(DatasetFramework dsFramework) throws DatasetManagementException, IOException {
    dsFramework.addInstance(Table.class.getName(), ConfigDataset.CONFIG_STORE_DATASET_INSTANCE_ID,
                            DatasetProperties.EMPTY);
  }

  @Override
  public void create(String namespace, String type, Config config) throws ConfigExistsException {
    Transactionals.execute(transactional, context -> {
      getConfigDataset(context).create(namespace, type, config);
    }, ConfigExistsException.class);
  }

  @Override
  public void createOrUpdate(String namespace, String type, Config config) {
    Transactionals.execute(transactional, context -> {
      getConfigDataset(context).createOrUpdate(namespace, type, config);
    });
  }

  @Override
  public void delete(String namespace, String type, String id) throws ConfigNotFoundException {
    Transactionals.execute(transactional, context -> {
      getConfigDataset(context).delete(namespace, type, id);
    }, ConfigNotFoundException.class);
  }

  @Override
  public List<Config> list(String namespace, String type) {
    return Transactionals.execute(transactional, context -> {
      return getConfigDataset(context).list(namespace, type);
    });
  }

  @Override
  public Config get(String namespace, String type, String id) throws ConfigNotFoundException {
    return Transactionals.execute(transactional, context -> {
      return getConfigDataset(context).get(namespace, type, id);
    }, ConfigNotFoundException.class);
  }

  @Override
  public void update(String namespace, String type, Config config) throws ConfigNotFoundException {
    Transactionals.execute(transactional, context -> {
      getConfigDataset(context).update(namespace, type, config);
    }, ConfigNotFoundException.class);
  }

  private ConfigDataset getConfigDataset(DatasetContext context) {
    return ConfigDataset.get(context, datasetFramework);
  }
}
