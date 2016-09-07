/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link StreamMetaStore} that access MDS directly.
 */
public final class MDSStreamMetaStore implements StreamMetaStore {

  // note: these constants should be same as in DefaultStore - this needs refactoring, but currently these pieces
  // dependent
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final String TYPE_STREAM = "stream";

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public MDSStreamMetaStore(DatasetFramework dsFramework, TransactionSystemClient txClient) {
    this.datasetFramework = dsFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  private MetadataStoreDataset getMetadataStore(DatasetContext context) throws IOException, DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, APP_META_INSTANCE_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new MetadataStoreDataset(table);
  }

  @Override
  public void addStream(Id.Stream streamId) throws Exception {
    addStream(streamId, null);
  }

  @Override
  public void addStream(final Id.Stream streamId, @Nullable final String description) throws Exception {
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        String desc = Optional.fromNullable(description).orNull();
        getMetadataStore(context).write(getKey(streamId), createStreamSpec(streamId, desc));
      }
    });
  }

  @Override
  public StreamSpecification getStream(final Id.Stream streamId) throws Exception {
    return Transactions.execute(transactional, new TxCallable<StreamSpecification>() {
      @Override
      public StreamSpecification call(DatasetContext context) throws Exception {
        return getMetadataStore(context).getFirst(getKey(streamId), StreamSpecification.class);
      }
    });
  }

  @Override
  public void removeStream(final Id.Stream streamId) throws Exception {
    transactional.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getMetadataStore(context).deleteAll(getKey(streamId));
      }
    });
  }

  @Override
  public boolean streamExists(final Id.Stream streamId) throws Exception {
    return getStream(streamId) != null;
  }

  @Override
  public List<StreamSpecification> listStreams(final Id.Namespace namespaceId) throws Exception {
    return Transactions.execute(transactional, new TxCallable<List<StreamSpecification>>() {
      @Override
      public List<StreamSpecification> call(DatasetContext context) throws Exception {
        return getMetadataStore(context).list(new MDSKey.Builder().add(TYPE_STREAM, namespaceId.getId()).build(),
                                              StreamSpecification.class);
      }
    });
  }

  @Override
  public Multimap<Id.Namespace, StreamSpecification> listStreams() throws Exception {
    return Transactions.execute(transactional, new TxCallable<Multimap<Id.Namespace, StreamSpecification>>() {
      @Override
      public Multimap<Id.Namespace, StreamSpecification> call(DatasetContext context) throws Exception {
        ImmutableMultimap.Builder<Id.Namespace, StreamSpecification> builder = ImmutableMultimap.builder();
        Map<MDSKey, StreamSpecification> streamSpecs =
          getMetadataStore(context).listKV(new MDSKey.Builder().add(TYPE_STREAM).build(), StreamSpecification.class);
        for (Map.Entry<MDSKey, StreamSpecification> streamSpecEntry : streamSpecs.entrySet()) {
          MDSKey.Splitter splitter = streamSpecEntry.getKey().split();
          // skip the first name ("stream")
          splitter.skipString();
          // Namespace id is the next part.
          String namespaceId = splitter.getString();
          builder.put(Id.Namespace.from(namespaceId), streamSpecEntry.getValue());
        }
        return builder.build();
      }
    });
  }

  private MDSKey getKey(Id.Stream streamId) {
    return new MDSKey.Builder().add(TYPE_STREAM, streamId.getNamespaceId(), streamId.getId()).build();
  }

  private StreamSpecification createStreamSpec(Id.Stream streamId, String description) {
    return new StreamSpecification.Builder().setName(streamId.getId()).setDescription(description).create();
  }
}
