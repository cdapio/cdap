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

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

/**
 * Default implementation of {@link ViewStore}.
 */
public final class MDSViewStore implements ViewStore {

  private static final Id.DatasetInstance STORE_DATASET_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.AppMetaStore.TABLE);
  private static final String TYPE_STREAM_VIEW = "stream.view";

  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework datasetFramework;

  @Inject
  public MDSViewStore(final DatasetFramework datasetFramework,
                      TransactionExecutorFactory executorFactory) {
    this.datasetFramework = datasetFramework;
    this.executorFactory = executorFactory;
  }

  private <T> T execute(TransactionExecutor.Function<ViewMetadataStoreDataset, T> func) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(
        datasetFramework, STORE_DATASET_ID,
        "table", DatasetProperties.EMPTY,
        DatasetDefinition.NO_ARGUMENTS, null);
      ViewMetadataStoreDataset viewDataset = new ViewMetadataStoreDataset(table);
      TransactionExecutor txExecutor = Transactions.createTransactionExecutor(executorFactory, viewDataset);
      return txExecutor.execute(func, viewDataset);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error accessing %s table", Constants.Stream.View.STORE_TABLE), e);
    }
  }

  @Override
  public boolean createOrUpdate(final Id.Stream.View viewId, final ViewSpecification spec) {
    return execute(new TransactionExecutor.Function<ViewMetadataStoreDataset, Boolean>() {
      @Override
      public Boolean apply(ViewMetadataStoreDataset mds) throws Exception {
        boolean created = !mds.exists(getKey(viewId));
        mds.write(getKey(viewId), new StreamViewEntry(viewId, spec));
        return created;
      }
    });
  }

  @Override
  public boolean exists(final Id.Stream.View viewId) {
    return execute(new TransactionExecutor.Function<ViewMetadataStoreDataset, Boolean>() {
      @Override
      public Boolean apply(ViewMetadataStoreDataset mds) throws Exception {
        return mds.exists(getKey(viewId));
      }
    });
  }

  @Override
  public void delete(final Id.Stream.View viewId) throws NotFoundException {
    boolean notFound = execute(new TransactionExecutor.Function<ViewMetadataStoreDataset, Boolean>() {
      @Override
      public Boolean apply(ViewMetadataStoreDataset mds) throws Exception {
        if (!mds.exists(getKey(viewId))) {
          return true;
        }
        mds.deleteAll(getKey(viewId));
        return false;
      }
    });

    if (notFound) {
      throw new NotFoundException(viewId);
    }
  }

  @Override
  public List<Id.Stream.View> list(final Id.Stream streamId) {
    List<StreamViewEntry> entries = execute(
      new TransactionExecutor.Function<ViewMetadataStoreDataset, List<StreamViewEntry>>() {
        @Override
        public List<StreamViewEntry> apply(ViewMetadataStoreDataset mds) throws Exception {
          return Objects.firstNonNull(
            mds.<StreamViewEntry>list(getKey(streamId), StreamViewEntry.class),
            ImmutableList.<StreamViewEntry>of());
        }
      });

    ImmutableList.Builder<Id.Stream.View> builder = ImmutableList.builder();
    builder.addAll(Collections2.transform(entries, new Function<StreamViewEntry, Id.Stream.View>() {
      @Override
      public Id.Stream.View apply(StreamViewEntry input) {
        return input.getId();
      }
    }));
    return builder.build();
  }

  @Override
  public ViewDetail get(final Id.Stream.View viewId) throws NotFoundException {
    StreamViewEntry entry = execute(
      new TransactionExecutor.Function<ViewMetadataStoreDataset, StreamViewEntry>() {
      @Override
      public StreamViewEntry apply(ViewMetadataStoreDataset mds) throws Exception {
        if (!mds.exists(getKey(viewId))) {
          return null;
        }
        return mds.get(getKey(viewId), StreamViewEntry.class);
      }
    });
    if (entry == null) {
      throw new NotFoundException(viewId);
    }
    return new ViewDetail(viewId.getId(), entry.getSpec());
  }

  private MDSKey getKey(Id.Stream id) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM_VIEW, id.getNamespaceId(), id.getId())
      .build();
  }

  private MDSKey getKey(Id.Stream.View id) {
    return new MDSKey.Builder()
      .add(TYPE_STREAM_VIEW, id.getNamespaceId(), id.getStreamId(), id.getId())
      .build();
  }

  private static final class StreamViewEntry {
    private final Id.Stream.View id;
    private final ViewSpecification spec;

    private StreamViewEntry(Id.Stream.View id, ViewSpecification spec) {
      this.id = id;
      this.spec = spec;
    }

    public Id.Stream.View getId() {
      return id;
    }

    public ViewSpecification getSpec() {
      return spec;
    }
  }
}
