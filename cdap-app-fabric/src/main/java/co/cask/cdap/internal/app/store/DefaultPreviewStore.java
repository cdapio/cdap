/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.store.PreviewStore;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.PreviewId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *  Default implementation of the {@link PreviewStore} that stores data in in-memory table
 */
public class DefaultPreviewStore implements PreviewStore {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewStore.class);
  private static final DatasetId PREVIEW_TABLE_INSTANCE_ID = new DatasetId(NamespaceId.SYSTEM.getNamespace(),
                                                                           PreviewDataset.PREVIEW_TABLE_NAME);

  private final DatasetFramework dsFramework;
  private final Supplier<PreviewDataset> previewDatasetSupplier;
  private final Supplier<TransactionExecutor> previewDatasetTx;
  private final MultiThreadDatasetCache dsCache;

  @Inject
  public DefaultPreviewStore(final TransactionExecutorFactory txExecutorFactory, DatasetFramework framework,
                             TransactionSystemClient txClient) {

    this.dsFramework = framework;
    this.dsCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(framework, null, null), txClient, NamespaceId.SYSTEM,
      ImmutableMap.<String, String>of(), null, null);

    this.previewDatasetSupplier = new Supplier<PreviewDataset>() {
      @Override
      public PreviewDataset get() {
        Table table = getCachedOrCreateTable(PREVIEW_TABLE_INSTANCE_ID.getDataset());
        return new PreviewDataset(table);
      }
    };

    this.previewDatasetTx = new Supplier<TransactionExecutor>() {
      @Override
      public TransactionExecutor get() {
        return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) previewDatasetSupplier.get()));
      }
    };

  }

  private Table getCachedOrCreateTable(String name) {
    try {
      return dsCache.getDataset(name);
    } catch (DatasetInstantiationException e) {
      try {
        DatasetsUtil.getOrCreateDataset(
          dsFramework, Id.DatasetInstance.from(Id.Namespace.SYSTEM, name), "table",
          DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
        return dsCache.getDataset(name);
      } catch (DatasetManagementException | IOException e1) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public synchronized void put(final PreviewId previewId, final String loggerName, final String propertyName,
                               final Object value) {
    previewDatasetTx.get().executeUnchecked(new TransactionExecutor.Function<PreviewDataset, Void>() {
      @Override
      public Void apply(PreviewDataset previewDS) throws Exception {
        previewDS.put(previewId, loggerName, propertyName, value);
        return null;
      }
    }, previewDatasetSupplier.get());
  }

  @Override
  public Map<String, List<String>> get(final PreviewId previewId, final String loggerName) {
    return previewDatasetTx.get().executeUnchecked(new TransactionExecutor.Function<PreviewDataset,
      Map<String, List<String>>>() {
      @Override
      public Map<String, List<String>> apply(PreviewDataset previewDS) throws Exception {
        return previewDS.get(previewId, loggerName);
      }
    }, previewDatasetSupplier.get());
  }

  @Override
  public void remove(PreviewId previewId) {

  }

  @VisibleForTesting
  @Override
  public void clear() throws IOException, DatasetManagementException {
    truncate(dsFramework.getAdmin(PREVIEW_TABLE_INSTANCE_ID.toId(), null));
  }

  private void truncate(DatasetAdmin admin) throws IOException {
    if (admin != null) {
      admin.truncate();
    }
  }
}
