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

package co.cask.cdap.namespace;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of {@link NamespaceMetaStore} that accesses MDS directly.
 */
public class MDSNamespaceMetaStore implements NamespaceMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceMetaStore.class);

  private static final String TYPE_NAMESPACE = "namespace";

  private Transactional<NamespaceMds, MetadataStoreDataset> txnl;

  @Inject
  public MDSNamespaceMetaStore(CConfiguration conf, final TransactionSystemClient txClient,
                               DatasetFramework framework) {
    final DatasetFramework dsFramework =
      new NamespacedDatasetFramework(framework, new DefaultDatasetNamespace(conf, Namespace.SYSTEM));

    txnl = Transactional.of(
      new TransactionExecutorFactory() {
        @Override
        public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
          return new DefaultTransactionExecutor(txClient, txAwares);
        }
      },
      new Supplier<NamespaceMds>() {
        @Override
        public NamespaceMds get() {
          try {
            Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, DefaultStore.APP_META_TABLE, "table",
                                                             DatasetProperties.EMPTY,
                                                             DatasetDefinition.NO_ARGUMENTS, null);
            return new NamespaceMds(new MetadataStoreDataset(mdsTable));
          } catch (Exception e) {
            LOG.error("Failed to access {} table", DefaultStore.APP_META_TABLE, e);
            throw Throwables.propagate(e);
          }
        }
      }
    );
  }

  @Override
  public void create(final NamespaceMeta metadata) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Void>() {
      @Override
      public Void apply(NamespaceMds input) throws Exception {
        input.namespaces.write(getKey(metadata.getName()), metadata);
        return null;
      }
    });
  }

  @Override
  public NamespaceMeta get(final Id.Namespace id) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(NamespaceMds input) throws Exception {
        return input.namespaces.get(getKey(id.getId()), NamespaceMeta.class);
      }
    });
  }

  @Override
  public void delete(final Id.Namespace id) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Void>() {
      @Override
      public Void apply(NamespaceMds input) throws Exception {
        input.namespaces.deleteAll(getKey(id.getId()));
        return null;
      }
    });
  }

  @Override
  public List<NamespaceMeta> list() throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, List<NamespaceMeta>>() {
      @Override
      public List<NamespaceMeta> apply(NamespaceMds input) throws Exception {
        return Lists.transform(input.namespaces.list(getKey(null), NamespaceMeta.class), new
          Function<NamespaceMeta, NamespaceMeta>() {

          @Nullable
          @Override
          public NamespaceMeta apply(NamespaceMeta namespaceMeta) {
            return namespaceMeta;
          }
        });
      }
    });
  }

  @Override
  public boolean exists(final Id.Namespace id) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Boolean>() {
      @Override
      public Boolean apply(NamespaceMds input) throws Exception {
        return input.namespaces.get(getKey(id.getId()), NamespaceMeta.class) != null;
      }
    });
  }

  private MetadataStoreDataset.Key getKey(@Nullable String name) {
    MetadataStoreDataset.Key.Builder builder = new MetadataStoreDataset.Key.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }

  private class NamespaceMds implements Iterable<MetadataStoreDataset> {
    private final MetadataStoreDataset namespaces;

    private NamespaceMds(MetadataStoreDataset metaTable) {
      this.namespaces = metaTable;
    }

    @Override
    public Iterator<MetadataStoreDataset> iterator() {
      return Iterators.singletonIterator(namespaces);
    }
  }
}
