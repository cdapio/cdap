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

package co.cask.cdap.store;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link NamespaceStore}.
 */
public class DefaultNamespaceStore implements NamespaceStore {

  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);

  private final DatasetFramework dsFramework;
  private final Transactional transactional;

  @Inject
  DefaultNamespaceStore(TransactionSystemClient txClient, DatasetFramework framework) {
    this.dsFramework = framework;
    this.transactional = Transactions.createTransactional(
      new MultiThreadDatasetCache(new SystemDatasetInstantiator(dsFramework), txClient,
                                  NamespaceId.SYSTEM, null, null, null)
    );
  }

  private NamespaceMDS getNamespaceMDS(DatasetContext datasetContext) throws IOException, DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, APP_META_INSTANCE_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new NamespaceMDS(table);
  }

  @Override
  @Nullable
  public NamespaceMeta create(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    try {
      return Transactions.execute(transactional, new TxCallable<NamespaceMeta>() {
        @Override
        public NamespaceMeta call(DatasetContext context) throws Exception {
          NamespaceMDS mds = getNamespaceMDS(context);
          NamespaceMeta existing = mds.get(metadata.getNamespaceId().toId());
          if (existing != null) {
            return existing;
          }
          mds.create(metadata);
          return null;
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public void update(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    try {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          NamespaceMDS mds = getNamespaceMDS(context);
          NamespaceMeta existing = mds.get(metadata.getNamespaceId().toId());
          if (existing != null) {
            mds.create(metadata);
          }
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  @Nullable
  public NamespaceMeta get(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    try {
      return Transactions.execute(transactional, new TxCallable<NamespaceMeta>() {
        @Override
        public NamespaceMeta call(DatasetContext context) throws Exception {
          return getNamespaceMDS(context).get(id);
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  @Nullable
  public NamespaceMeta delete(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    try {
      return Transactions.execute(transactional, new TxCallable<NamespaceMeta>() {
        @Override
        public NamespaceMeta call(DatasetContext context) throws Exception {
          NamespaceMDS mds = getNamespaceMDS(context);
          NamespaceMeta existing = mds.get(id);
          if (existing != null) {
            mds.delete(id);
          }
          return existing;
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  @Override
  public List<NamespaceMeta> list() {
    try {
      return Transactions.execute(transactional, new TxCallable<List<NamespaceMeta>>() {
        @Override
        public List<NamespaceMeta> call(DatasetContext context) throws Exception {
          return getNamespaceMDS(context).list();
        }
      });
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }
}
