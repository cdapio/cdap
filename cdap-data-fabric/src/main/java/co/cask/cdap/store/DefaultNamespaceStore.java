/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link NamespaceStore}.
 */
public class DefaultNamespaceStore implements NamespaceStore {

  private static final Id.DatasetInstance APP_META_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.AppMetaStore.TABLE);

  private final Supplier<NamespaceMDS> apps;
  private final Supplier<TransactionExecutor> appsTx;
  private final DatasetFramework dsFramework;
  private final MultiThreadDatasetCache dsCache;

  @Inject
  DefaultNamespaceStore(
    final TransactionExecutorFactory txExecutorFactory,
    TransactionSystemClient txClient,
    DatasetFramework framework) {
    this.dsFramework = framework;
    this.dsCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, null, null), txClient, NamespaceId.SYSTEM, null, null, null);
    this.apps =
      new Supplier<NamespaceMDS>() {
        @Override
        public NamespaceMDS get() {
          Table table;
          try {
            table = dsCache.getDataset(APP_META_INSTANCE_ID.getId());
          } catch (DatasetInstantiationException e) {
            try {
              DatasetsUtil.getOrCreateDataset(
                dsFramework, APP_META_INSTANCE_ID, "table",
                DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
              table = dsCache.getDataset(APP_META_INSTANCE_ID.getId());
            } catch (DatasetManagementException | IOException e1) {
              throw Throwables.propagate(e1);
            }
          }
          return new NamespaceMDS(table);
        }
      };
    this.appsTx = new Supplier<TransactionExecutor>() {
      @Override
      public TransactionExecutor get() {
        return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) apps.get()));
      }
    };
  }

  @Override
  @Nullable
  public NamespaceMeta create(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<NamespaceMDS, NamespaceMeta>() {
        @Override
        public NamespaceMeta apply(NamespaceMDS mds) throws Exception {
          Id.Namespace namespace = Id.Namespace.from(metadata.getName());
          NamespaceMeta existing = mds.get(namespace);
          if (existing != null) {
            return existing;
          }
          mds.create(metadata);
          return null;
        }
      }, apps.get());
  }

  @Override
  public void update(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Procedure<NamespaceMDS>() {
        @Override
        public void apply(NamespaceMDS mds) throws Exception {
          NamespaceMeta existing = mds.get(Id.Namespace.from(metadata.getName()));
          if (existing != null) {
            mds.create(metadata);
          }
        }
      }, apps.get());
  }

  @Override
  @Nullable
  public NamespaceMeta get(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<NamespaceMDS, NamespaceMeta>() {
        @Override
        public NamespaceMeta apply(NamespaceMDS mds) throws Exception {
          return mds.get(id);
        }
      }, apps.get());
  }

  @Override
  @Nullable
  public NamespaceMeta delete(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<NamespaceMDS, NamespaceMeta>() {
        @Override
        public NamespaceMeta apply(NamespaceMDS mds) throws Exception {
          NamespaceMeta existing = mds.get(id);
          if (existing != null) {
            mds.delete(id);
          }
          return existing;
        }
      }, apps.get());
  }

  @Override
  public List<NamespaceMeta> list() {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<NamespaceMDS, List<NamespaceMeta>>() {
        @Override
        public List<NamespaceMeta> apply(NamespaceMDS mds) throws Exception {
          return mds.list();
        }
      }, apps.get());
  }

}
