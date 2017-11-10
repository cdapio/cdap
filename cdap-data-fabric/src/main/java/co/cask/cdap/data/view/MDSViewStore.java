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

package co.cask.cdap.data.view;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ViewStore}.
 */
public final class MDSViewStore implements ViewStore {

  private static final DatasetId STORE_DATASET_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final String TYPE_STREAM_VIEW = "stream.view";

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public MDSViewStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  private ViewMetadataStoreDataset getViewDataset(DatasetContext datasetContext) throws IOException,
                                                                                        DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, STORE_DATASET_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new ViewMetadataStoreDataset(table);
  }

  @Override
  public boolean createOrUpdate(final StreamViewId viewId, final ViewSpecification spec) {
    return Transactionals.execute(transactional, context -> {
      ViewMetadataStoreDataset mds = getViewDataset(context);
      boolean created = !mds.exists(getKey(viewId));
      mds.write(getKey(viewId), new StreamViewEntry(viewId, spec));
      return created;
    });
  }

  @Override
  public boolean exists(final StreamViewId viewId) {
    return Transactionals.execute(transactional, context -> {
      return getViewDataset(context).exists(getKey(viewId));
    });
  }

  @Override
  public void delete(final StreamViewId viewId) throws NotFoundException {
    Transactionals.execute(transactional, context -> {
      ViewMetadataStoreDataset mds = getViewDataset(context);
      MDSKey key = getKey(viewId);
      if (!mds.exists(key)) {
        throw new NotFoundException(viewId);
      }
      mds.deleteAll(key);
    }, NotFoundException.class);
  }

  @Override
  public List<StreamViewId> list(final StreamId streamId) {
    return Transactionals.execute(transactional, context -> {
      List<StreamViewEntry> views = getViewDataset(context).list(getKey(streamId), StreamViewEntry.class);
      return views.stream().map(StreamViewEntry::getId).collect(Collectors.toList());
    });
  }

  @Override
  public ViewDetail get(final StreamViewId viewId) throws NotFoundException {
    return Transactionals.execute(transactional, context -> {
      StreamViewEntry viewEntry = getViewDataset(context).get(getKey(viewId), StreamViewEntry.class);
      if (viewEntry == null) {
        throw new NotFoundException(viewId);
      }
      return new ViewDetail(viewId.getEntityName(), viewEntry.getSpec());
    }, NotFoundException.class);
  }

  private MDSKey getKey(StreamId id) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM_VIEW, id.getNamespace(), id.getEntityName())
      .build();
  }

  private MDSKey getKey(StreamViewId id) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM_VIEW, id.getNamespace(), id.getStream(), id.getEntityName())
      .build();
  }

  private static final class StreamViewEntry {
    // This must use the deprecated stream view Id class, since this class is used for serialization and deserialization
    // in the persistence layer. See CDAP-7844 for more details.
    private final Id.Stream.View id;
    private final ViewSpecification spec;

    private StreamViewEntry(StreamViewId id, ViewSpecification spec) {
      this.id = id.toId();
      this.spec = spec;
    }

    public StreamViewId getId() {
      return id.toEntityId();
    }

    public ViewSpecification getSpec() {
      return spec;
    }
  }
}
