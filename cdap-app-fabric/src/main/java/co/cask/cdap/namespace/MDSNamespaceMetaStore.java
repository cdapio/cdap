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
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link co.cask.cdap.namespace.NamespaceMetaStore} that accesses MDS directly
 */
public class MDSNamespaceMetaStore implements NamespaceMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceMetaStore.class);

  private static final String NAMESPACE_META_TABLE = "namespace.meta";
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
            Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, NAMESPACE_META_TABLE, "table",
                                                             DatasetProperties.EMPTY,
                                                             DatasetDefinition.NO_ARGUMENTS, null);
            return new NamespaceMds(new MetadataStoreDataset(mdsTable));
          } catch (Exception e) {
            LOG.error("Failed to access app.meta table", e);
            throw Throwables.propagate(e);
          }
        }
      }
    );
  }

  @Override
  public void create(final String name, final String displayName, final String description) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Void>() {
      @Override
      public Void apply(NamespaceMds input) throws Exception {
        input.namespaces.write(getKey(name), createNamespaceSpec(name, displayName, description));
        return null;
      }
    });
  }

  @Override
  public NamespaceMetadata get(final String name) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, NamespaceMetadata>() {
      @Override
      public NamespaceMetadata apply(NamespaceMds input) throws Exception {
        return input.namespaces.get(getKey(name), NamespaceMetadata.class);
      }
    });
  }

  @Override
  public void delete(final String name) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Void>() {
      @Override
      public Void apply(NamespaceMds input) throws Exception {
        input.namespaces.deleteAll(getKey(name));
        return null;
      }
    });
  }

  @Override
  public List<NamespaceMetadata> list() {
    return null;
  }

  @Override
  public boolean exists(final String name) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<NamespaceMds, Boolean>() {
      @Override
      public Boolean apply(NamespaceMds input) throws Exception {
        return input.namespaces.get(getKey(name), NamespaceMetadata.class) != null;
      }
    });
  }

  private MetadataStoreDataset.Key getKey(String name) {
    return new MetadataStoreDataset.Key.Builder().add(TYPE_NAMESPACE, name).build();
  }

  private NamespaceMetadata createNamespaceSpec(String name, String displayName, String description) {
    return new NamespaceMetadata.Builder().setName(name).setDisplayName(displayName).setDescription(description).build();
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
