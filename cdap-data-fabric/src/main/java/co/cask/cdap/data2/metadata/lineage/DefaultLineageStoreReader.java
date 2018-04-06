/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Implementation of {@link LineageStoreReader} for reading lineage information from {@link LineageDataset}.
 */
public class DefaultLineageStoreReader implements LineageStoreReader {
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final DatasetId lineageDatasetId;

  @Inject
  DefaultLineageStoreReader(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this(datasetFramework, txClient, LineageDataset.LINEAGE_DATASET_ID);
  }

  @VisibleForTesting
  public DefaultLineageStoreReader(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                   DatasetId lineageDatasetId) {
    this.datasetFramework = datasetFramework;
    this.lineageDatasetId = lineageDatasetId;
    this.transactional = Transactions.createTransactional(new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
      NamespaceId.SYSTEM, ImmutableMap.of(), null, null));
  }

  /**
   * @return a set of entities (program and data it accesses) associated with a program run.
   */
  @Override
  public Set<NamespacedEntityId> getEntitiesForRun(final ProgramRunId run) {
    return execute(input -> input.getEntitiesForRun(run));
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
    return execute(input -> input.getRelations(datasetInstance, start, end, filter));
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
    return execute(input -> input.getRelations(stream, start, end, filter));
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
    return execute(input -> input.getRelations(program, start, end, filter));
  }

  private <T> T execute(TransactionExecutor.Function<LineageDataset, T> func) {
    return Transactionals.execute(transactional, context -> {
      LineageDataset lineageDataset = LineageDataset.getLineageDataset(context, datasetFramework, lineageDatasetId);
      return func.apply(lineageDataset);
    });
  }
}
