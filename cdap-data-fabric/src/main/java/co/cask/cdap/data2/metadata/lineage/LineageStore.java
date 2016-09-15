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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Store for storing/retrieving lineage information for a Dataset.
 */
public class LineageStore implements LineageStoreReader, LineageStoreWriter {
  private static final DatasetId LINEAGE_DATASET_ID = NamespaceId.SYSTEM.dataset("lineage");

  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework datasetFramework;
  private final DatasetId lineageDatasetId;

  @Inject
  public LineageStore(TransactionExecutorFactory executorFactory, DatasetFramework datasetFramework) {
    this(executorFactory, datasetFramework, LINEAGE_DATASET_ID);
  }

  @VisibleForTesting
  public LineageStore(TransactionExecutorFactory executorFactory, DatasetFramework datasetFramework,
                      DatasetId lineageDatasetId) {
    this.executorFactory = executorFactory;
    this.datasetFramework = datasetFramework;
    this.lineageDatasetId = lineageDatasetId;
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetInstance, AccessType accessType,
                        long accessTimeMillis) {
    addAccess(run, datasetInstance, accessType, accessTimeMillis, null);
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  @Override
  public void addAccess(final ProgramRunId run, final DatasetId datasetInstance,
                        final AccessType accessType, final long accessTimeMillis,
                        @Nullable final NamespacedEntityId component) {
    execute(new TransactionExecutor.Procedure<LineageDataset>() {
      @Override
      public void apply(LineageDataset input) throws Exception {
        input.addAccess(run, datasetInstance, accessType, accessTimeMillis, component);
      }
    });
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   */
  @Override
  public void addAccess(ProgramRunId run, StreamId stream, AccessType accessType, long accessTimeMillis) {
    addAccess(run, stream, accessType, accessTimeMillis, null);
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param accessTimeMillis time of access
   * @param component program component such as flowlet id, etc.
   */
  @Override
  public void addAccess(final ProgramRunId run, final StreamId stream,
                        final AccessType accessType, final long accessTimeMillis,
                        @Nullable final NamespacedEntityId component) {
    execute(new TransactionExecutor.Procedure<LineageDataset>() {
      @Override
      public void apply(LineageDataset input) throws Exception {
        input.addAccess(run, stream, accessType, accessTimeMillis, component);
      }
    });
  }

  /**
   * @return a set of entities (program and data it accesses) associated with a program run.
   */
  @Override
  public Set<NamespacedEntityId> getEntitiesForRun(final ProgramRunId run) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<NamespacedEntityId>>() {
      @Override
      public Set<NamespacedEntityId> apply(LineageDataset input) throws Exception {
        return input.getEntitiesForRun(run);
      }
    });
  }

  /**
   * Fetch program-dataset access information for a dataset for a given period.
   *
   * @param datasetInstance dataset for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  @Override
  public Set<Relation> getRelations(final DatasetId datasetInstance, final long start, final long end,
                                    final Predicate<Relation> filter) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<Relation>>() {
      @Override
      public Set<Relation> apply(LineageDataset input) throws Exception {
        return input.getRelations(datasetInstance, start, end, filter);
      }
    });
  }

  /**
   * Fetch program-stream access information for a dataset for a given period.
   *
   * @param stream stream for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-stream access information
   */
  @Override
  public Set<Relation> getRelations(final StreamId stream, final long start, final long end,
                                    final Predicate<Relation> filter) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<Relation>>() {
      @Override
      public Set<Relation> apply(LineageDataset input) throws Exception {
        return input.getRelations(stream, start, end, filter);
      }
    });
  }

  /**
   * Fetch program-dataset access information for a program for a given period.
   *
   * @param program program for which to fetch access information
   * @param start start time period
   * @param end end time period
   * @param filter filter to be applied on result set
   * @return program-dataset access information
   */
  @Override
  public Set<Relation> getRelations(final ProgramId program, final long start, final long end,
                                    final Predicate<Relation> filter) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<Relation>>() {
      @Override
      public Set<Relation> apply(LineageDataset input) throws Exception {
        return input.getRelations(program, start, end, filter);
      }
    });
  }

  /**
   * @return a set of access times (for program and data it accesses) associated with a program run.
   */
  @VisibleForTesting
  public List<Long> getAccessTimesForRun(final ProgramRunId run) {
    return execute(new TransactionExecutor.Function<LineageDataset, List<Long>>() {
      @Override
      public List<Long> apply(LineageDataset input) throws Exception {
        return input.getAccessTimesForRun(run);
      }
    });
  }

  private <T> T execute(TransactionExecutor.Function<LineageDataset, T> func) {
    LineageDataset lineageDataset = newLineageDataset();
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(executorFactory, lineageDataset);
    return txExecutor.executeUnchecked(func, lineageDataset);
  }

  private void execute(TransactionExecutor.Procedure<LineageDataset> func) {
    LineageDataset lineageDataset = newLineageDataset();
    TransactionExecutor txExecutor = Transactions.createTransactionExecutor(executorFactory, lineageDataset);
    txExecutor.executeUnchecked(func, lineageDataset);
  }

  private LineageDataset newLineageDataset() {
    try {
      return DatasetsUtil.getOrCreateDataset(
        datasetFramework, lineageDatasetId, LineageDataset.class.getName(),
        DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework}. Used by the upgrade tool to upgrade Lineage Dataset.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(LineageDataset.class.getName(), LINEAGE_DATASET_ID, DatasetProperties.EMPTY);
  }
}
