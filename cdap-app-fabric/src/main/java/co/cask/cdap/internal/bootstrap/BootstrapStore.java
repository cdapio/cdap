/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.bootstrap;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;

/**
 * Fetches and stores bootstrap state. Making the store in charge of starting transactions is generally discouraged.
 * However, this store contains a single key that is isolated from everything else, so it is easier to have it start
 * transactions.
 */
public class BootstrapStore {
  private static final MDSKey KEY = new MDSKey.Builder().add("boot").build();
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  BootstrapStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * @return whether the CDAP instance is bootstrapped.
   */
  public boolean isBootstrapped() {
    return Transactionals.execute(transactional, dsContext -> {
      MetadataStoreDataset ds = get(dsContext, datasetFramework);
      return ds.exists(KEY);
    });
  }

  /**
   * Mark the CDAP instance as bootstrapped.
   */
  public void bootstrapped() {
    Transactionals.execute(transactional, dsContext -> {
      MetadataStoreDataset ds = get(dsContext, datasetFramework);
      ds.write(KEY, Boolean.TRUE);
    });
  }

  /**
   * Clear bootstrap state. This should only be called in tests.
   */
  @VisibleForTesting
  void clear() {
    Transactionals.execute(transactional, dsContext -> {
      MetadataStoreDataset ds = get(dsContext, datasetFramework);
      ds.delete(KEY);
    });
  }

  private static MetadataStoreDataset get(DatasetContext datasetContext, DatasetFramework dsFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, AppMetadataStore.APP_META_INSTANCE_ID,
                                                    Table.class.getName(),
                                                    DatasetProperties.EMPTY);
      return new MetadataStoreDataset(table);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

}
