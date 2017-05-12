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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Store program -> dataset/stream usage information.
 *
 * TODO: Reduce duplication between this and {@link UsageDataset}.
 */
public class DefaultUsageRegistry implements UsageRegistry {

  private static final DatasetId USAGE_INSTANCE_ID = NamespaceId.SYSTEM.dataset("usage.registry");

  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework datasetFramework;

  @Inject
  DefaultUsageRegistry(TransactionExecutorFactory executorFactory, DatasetFramework datasetFramework) {
    this.executorFactory = executorFactory;
    this.datasetFramework = datasetFramework;
  }

  protected <T> T execute(TransactionExecutor.Function<UsageDataset, T> func) {
    UsageDataset usageDataset = getOrCreateUsageDataset();
    return Transactions.createTransactionExecutor(executorFactory, usageDataset)
      .executeUnchecked(func, usageDataset);
  }

  protected void execute(TransactionExecutor.Procedure<UsageDataset> func) {
    UsageDataset usageDataset = getOrCreateUsageDataset();
    Transactions.createTransactionExecutor(executorFactory, usageDataset)
      .executeUnchecked(func, usageDataset);
  }

  private UsageDataset getOrCreateUsageDataset() {
    try {
      return DatasetsUtil.getOrCreateDataset(
        datasetFramework, USAGE_INSTANCE_ID, UsageDataset.class.getSimpleName(),
        DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // TODO: javadocs aren't needed in implementation; just in interface...
  /**
   * Registers usage of a stream by multiple ids.
   *
   * @param users    the users of the stream
   * @param streamId the stream
   */
  @Override
  public void registerAll(final Iterable<? extends EntityId> users, final StreamId streamId) {
    for (EntityId user : users) {
      register(user, streamId);
    }
  }

  /**
   * Register usage of a stream by an id.
   *
   * @param user     the user of the stream
   * @param streamId the stream
   */
  @Override
  public void register(EntityId user, StreamId streamId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, streamId);
    }
  }

  /**
   * Registers usage of a stream by multiple ids.
   *
   * @param users     the users of the stream
   * @param datasetId the stream
   */
  @Override
  public void registerAll(final Iterable<? extends EntityId> users, final DatasetId datasetId) {
    for (EntityId user : users) {
      register(user, datasetId);
    }
  }

  /**
   * Registers usage of a dataset by multiple ids.
   *
   * @param user      the user of the dataset
   * @param datasetId the dataset
   */
  @Override
  public void register(EntityId user, DatasetId datasetId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, datasetId);
    }
  }

  /**
   * Registers usage of a dataset by a program.
   *
   * @param programId         program
   * @param datasetInstanceId dataset
   */
  @Override
  public void register(final ProgramId programId, final DatasetId datasetInstanceId) {
    execute(new TransactionExecutor.Procedure<UsageDataset>() {
      @Override
      public void apply(UsageDataset usageDataset) throws Exception {
        usageDataset.register(programId, datasetInstanceId);
      }
    });
  }

  /**
   * Internal method to register usage of a dataset by a program, called from the cache loader.
   *
   * @param programId program
   * @param datasetInstanceId  dataset
   */
  private void doRegister(final ProgramId programId, final DatasetId datasetInstanceId) {
    execute(new TransactionExecutor.Procedure<UsageDataset>() {
      @Override
      public void apply(UsageDataset usageDataset) throws Exception {
        usageDataset.register(programId, datasetInstanceId);
      }
    });
  }

  /**
   * Registers usage of a stream by a program.
   *
   * @param programId program
   * @param streamId  stream
   */
  @Override
  public void register(final ProgramId programId, final StreamId streamId) {
    execute(new TransactionExecutor.Procedure<UsageDataset>() {
      @Override
      public void apply(UsageDataset usageDataset) throws Exception {
        usageDataset.register(programId, streamId);
      }
    });
  }

  /**
   * Unregisters all usage information of an application.
   *
   * @param applicationId application
   */
  @Override
  public void unregister(final ApplicationId applicationId) {
    execute(new TransactionExecutor.Procedure<UsageDataset>() {
      @Override
      public void apply(UsageDataset usageDataset) throws Exception {
        usageDataset.unregister(applicationId);
      }
    });
  }

  @Override
  public Set<DatasetId> getDatasets(final ApplicationId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<DatasetId>>() {
      @Override
      public Set<DatasetId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getDatasets(id);
      }
    });
  }

  @Override
  public Set<StreamId> getStreams(final ApplicationId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<StreamId>>() {
      @Override
      public Set<StreamId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getStreams(id);
      }
    });
  }

  @Override
  public Set<DatasetId> getDatasets(final ProgramId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<DatasetId>>() {
      @Override
      public Set<DatasetId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getDatasets(id);
      }
    });
  }

  @Override
  public Set<StreamId> getStreams(final ProgramId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<StreamId>>() {
      @Override
      public Set<StreamId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getStreams(id);
      }
    });
  }

  @Override
  public Set<ProgramId> getPrograms(final StreamId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<ProgramId>>() {
      @Override
      public Set<ProgramId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getPrograms(id);
      }
    });
  }

  @Override
  public Set<ProgramId> getPrograms(final DatasetId id) {
    return execute(new TransactionExecutor.Function<UsageDataset, Set<ProgramId>>() {
      @Override
      public Set<ProgramId> apply(UsageDataset usageDataset) throws Exception {
        return usageDataset.getPrograms(id);
      }
    });
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by usage registry.
   *
   * @param datasetFramework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework datasetFramework) throws IOException, DatasetManagementException {
    datasetFramework.addInstance(UsageDataset.class.getSimpleName(), USAGE_INSTANCE_ID, DatasetProperties.EMPTY);
  }
}
