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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Store for storing/retrieving lineage information for a Dataset.
 */
public class LineageStore {
  private static final Id.DatasetInstance LINEAGE_DATASET_ID = Id.DatasetInstance.from(Id.Namespace.SYSTEM, "lineage");

  private final TransactionExecutorFactory executorFactory;
  private final DatasetFramework datasetFramework;
  private final Id.DatasetInstance lineageDatasetId;

  @Inject
  public LineageStore(TransactionExecutorFactory executorFactory,
                      @Named(DataSetsModules.BASIC_DATASET_FRAMEWORK) DatasetFramework datasetFramework) {
    this(executorFactory, datasetFramework, LINEAGE_DATASET_ID);
  }

  @VisibleForTesting
  public LineageStore(TransactionExecutorFactory executorFactory, DatasetFramework datasetFramework,
                      Id.DatasetInstance lineageDatasetId) {
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
   * @param metadata metadata to store for the access
   */
  public void addAccess(Id.Run run, Id.DatasetInstance datasetInstance, AccessType accessType,
                        Set<MetadataRecord> metadata) {
    addAccess(run, datasetInstance, accessType, metadata, null);
  }

  /**
   * Add a program-dataset access.
   *
   * @param run program run information
   * @param datasetInstance dataset accessed by the program
   * @param accessType access type
   * @param metadata metadata to store for the access
   * @param component program component such as flowlet id, etc.
   */
  public void addAccess(final Id.Run run, final Id.DatasetInstance datasetInstance,
                        final AccessType accessType, final Set<MetadataRecord> metadata,
                        @Nullable final Id.NamespacedId component) {
    execute(new TransactionExecutor.Procedure<LineageDataset>() {
      @Override
      public void apply(LineageDataset input) throws Exception {
        input.addAccess(run, datasetInstance, accessType, metadata, component);
      }
    });
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param metadata metadata to store for the access
   */
  public void addAccess(Id.Run run, Id.Stream stream, AccessType accessType, Set<MetadataRecord> metadata) {
    addAccess(run, stream, accessType, metadata, null);
  }

  /**
   * Add a program-stream access.
   *
   * @param run program run information
   * @param stream stream accessed by the program
   * @param accessType access type
   * @param metadata metadata to store for the access
   * @param component program component such as flowlet id, etc.
   */
  public void addAccess(final Id.Run run, final Id.Stream stream,
                        final AccessType accessType, final Set<MetadataRecord> metadata,
                        @Nullable final Id.NamespacedId component) {
    execute(new TransactionExecutor.Procedure<LineageDataset>() {
      @Override
      public void apply(LineageDataset input) throws Exception {
        input.addAccess(run, stream, accessType, metadata, component);
      }
    });
  }

  /**
   * @return a set of {@link MetadataRecord}s (associated with both program and data it accesses)
   * for a given program run.
   */
  public Set<MetadataRecord> getRunMetadata(final Id.Run run) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<MetadataRecord>>() {
      @Override
      public Set<MetadataRecord> apply(LineageDataset input) throws Exception {
        return input.getRunMetadata(run);
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
  public Set<Relation> getRelations(final Id.DatasetInstance datasetInstance, final long start, final long end,
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
  public Set<Relation> getRelations(final Id.Stream stream, final long start, final long end,
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
  public Set<Relation> getRelations(final Id.Program program, final long start, final long end,
                                    final Predicate<Relation> filter) {
    return execute(new TransactionExecutor.Function<LineageDataset, Set<Relation>>() {
      @Override
      public Set<Relation> apply(LineageDataset input) throws Exception {
        return input.getRelations(program, start, end, filter);
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
}
