/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Basic implementation of {@link UsageRegistry} that read/write to {@link UsageDataset} directly.
 * This class should only be used in master/standalone mode, but not in distributed container.
 */
public class BasicUsageRegistry implements UsageRegistry {

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final DatasetId usageDatasetId;

  @Inject
  BasicUsageRegistry(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this(datasetFramework, txClient, UsageDataset.USAGE_INSTANCE_ID);
  }

  @VisibleForTesting
  public BasicUsageRegistry(DatasetFramework datasetFramework,
                            TransactionSystemClient txClient, DatasetId usageDatasetId) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );
    this.usageDatasetId = usageDatasetId;
  }

  protected <T> T call(Function<UsageDataset, T> func) {
    return Transactionals.execute(transactional, context -> {
      return func.apply(UsageDataset.getUsageDataset(context, datasetFramework, usageDatasetId));
    });
  }

  protected void execute(Consumer<UsageDataset> consumer) {
    Transactionals.execute(transactional, context -> {
      consumer.accept(UsageDataset.getUsageDataset(context, datasetFramework, usageDatasetId));
    });
  }

  @Override
  public void register(EntityId user, StreamId streamId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, streamId);
    }
  }

  @Override
  public void register(EntityId user, DatasetId datasetId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, datasetId);
    }
  }

  @Override
  public void register(final ProgramId programId, final DatasetId datasetInstanceId) {
    execute(usageDataset -> usageDataset.register(programId, datasetInstanceId));
  }

  @Override
  public void register(final ProgramId programId, final StreamId streamId) {
    execute(usageDataset -> usageDataset.register(programId, streamId));
  }

  @Override
  public void unregister(final ApplicationId applicationId) {
    execute(usageDataset -> usageDataset.unregister(applicationId));
  }

  @Override
  public Set<DatasetId> getDatasets(final ApplicationId id) {
    return call(usageDataset -> usageDataset.getDatasets(id));
  }

  @Override
  public Set<StreamId> getStreams(final ApplicationId id) {
    return call(usageDataset -> usageDataset.getStreams(id));
  }

  @Override
  public Set<DatasetId> getDatasets(final ProgramId id) {
    return call(usageDataset -> usageDataset.getDatasets(id));
  }

  @Override
  public Set<StreamId> getStreams(final ProgramId id) {
    return call(usageDataset -> usageDataset.getStreams(id));
  }

  @Override
  public Set<ProgramId> getPrograms(final StreamId id) {
    return call(usageDataset -> usageDataset.getPrograms(id));
  }

  @Override
  public Set<ProgramId> getPrograms(final DatasetId id) {
    return call(usageDataset -> usageDataset.getPrograms(id));
  }
}
