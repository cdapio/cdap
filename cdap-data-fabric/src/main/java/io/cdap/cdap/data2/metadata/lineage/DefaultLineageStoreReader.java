/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.lineage;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.tephra.TransactionExecutor;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Implementation of {@link LineageStoreReader} for reading lineage information from {@link LineageTable}.
 */
public class DefaultLineageStoreReader implements LineageStoreReader {

  private final TransactionRunner transactionRunner;

  @Inject
  @VisibleForTesting
  public DefaultLineageStoreReader(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
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

  private <T> T execute(TransactionExecutor.Function<LineageTable, T> func) {
    return TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      return func.apply(lineageTable);
    });
  }
}
