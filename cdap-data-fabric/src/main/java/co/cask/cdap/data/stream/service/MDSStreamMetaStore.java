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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
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
import co.cask.cdap.proto.Id;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link StreamMetaStore} that access MDS directly.
 */
public final class MDSStreamMetaStore implements StreamMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(MDSStreamMetaStore.class);

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final String STREAM_META_TABLE = "app.meta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_NAMESPACE = "namespace";

  private Transactional<StreamMds, MetadataStoreDataset> txnl;

  @Inject
  public MDSStreamMetaStore(CConfiguration conf, final TransactionSystemClient txClient, DatasetFramework framework) {

    final DatasetFramework dsFramework =
      new NamespacedDatasetFramework(framework, new DefaultDatasetNamespace(conf, Namespace.SYSTEM));

    txnl = Transactional.of(
        new TransactionExecutorFactory() {
          @Override
          public TransactionExecutor createExecutor(Iterable<TransactionAware> transactionAwares) {
            return new DefaultTransactionExecutor(txClient, transactionAwares);
          }},
        new Supplier<StreamMds>() {
          @Override
          public StreamMds get() {
            try {
              Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, STREAM_META_TABLE, "table",
                                                               DatasetProperties.EMPTY,
                                                               DatasetDefinition.NO_ARGUMENTS, null);

              return new StreamMds(new MetadataStoreDataset(mdsTable));
            } catch (Exception e) {
              LOG.error("Failed to access app.meta table", e);
              throw Throwables.propagate(e);
            }
          }
        });
  }

  @Override
  public void addStream(final String accountId, final String streamName) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.write(getKey(accountId, streamName),
                          createStreamSpec(streamName));
        return null;
      }
    });
  }

  @Override
  public void removeStream(final String accountId, final String streamName) throws Exception {
    txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Void>() {
      @Override
      public Void apply(StreamMds mds) throws Exception {
        mds.streams.deleteAll(getKey(accountId, streamName));
        return null;
      }
    });
  }

  @Override
  public boolean streamExists(final String accountId, final String streamName) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, Boolean>() {
      @Override
      public Boolean apply(StreamMds mds) throws Exception {
        return mds.streams.get(getKey(accountId, streamName), StreamSpecification.class) != null;
      }
    });
  }

  @Override
  public List<StreamSpecification> listStreams(final String accountId) throws Exception {
    return txnl.executeUnchecked(new TransactionExecutor.Function<StreamMds, List<StreamSpecification>>() {
      @Override
      public List<StreamSpecification> apply(StreamMds mds) throws Exception {
        return mds.streams.list(new MetadataStoreDataset.Key.Builder().add(TYPE_STREAM, accountId).build(),
                                StreamSpecification.class);
      }
    });
  }

  @Override
  public Multimap<Id.Namespace, StreamSpecification> listStreams() throws Exception {
    return txnl.executeUnchecked(
      new TransactionExecutor.Function<StreamMds, Multimap<Id.Namespace, StreamSpecification>>() {
        @Override
        public Multimap<Id.Namespace, StreamSpecification> apply(StreamMds mds) throws Exception {
          ImmutableMultimap.Builder<Id.Namespace, StreamSpecification> builder = ImmutableMultimap.builder();
          Map<MetadataStoreDataset.Key, StreamSpecification> streamSpecs =
            mds.streams.listKV(new MetadataStoreDataset.Key.Builder().add(TYPE_STREAM).build(),
                               StreamSpecification.class);
          for (Map.Entry<MetadataStoreDataset.Key, StreamSpecification> streamSpecEntry : streamSpecs.entrySet()) {
            List<byte[]> keyParts = streamSpecEntry.getKey().split();
            // Namespace id is the key part with index 1.
            String namespaceId = Bytes.toString(keyParts.get(1));
            builder.put(Id.Namespace.from(namespaceId), streamSpecEntry.getValue());
          }
          return builder.build();
        }
      });
  }

  private MetadataStoreDataset.Key getKey(String accountId, String streamName) {
    return new MetadataStoreDataset.Key.Builder().add(TYPE_STREAM, accountId, streamName).build();
  }

  private StreamSpecification createStreamSpec(String streamName) {
    return new StreamSpecification.Builder().setName(streamName).create();
  }

  private static final class StreamMds implements Iterable<MetadataStoreDataset> {
    private final MetadataStoreDataset streams;

    private StreamMds(MetadataStoreDataset metaTable) {
      this.streams = metaTable;
    }

    @Override
    public Iterator<MetadataStoreDataset> iterator() {
      return Iterators.singletonIterator(streams);
    }
  }
}
