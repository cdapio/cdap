/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.registry;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.util.Set;

/**
 * Basic implementation of {@link UsageRegistry} that read/write to {@link UsageTable} directly.
 * This class should only be used in master/standalone mode, but not in distributed container.
 */
public class BasicUsageRegistry implements UsageRegistry {

  private TransactionRunner transactionRunner;

  @Inject
  @VisibleForTesting
  public BasicUsageRegistry(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void register(EntityId user, DatasetId datasetId) {
    if (user instanceof ProgramId) {
      register((ProgramId) user, datasetId);
    }
  }

  @Override
  public void register(final ProgramId programId, final DatasetId datasetInstanceId) {
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.register(programId, datasetInstanceId);
    });
  }

  @Override
  public void unregister(final ApplicationId applicationId) {
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.unregister(applicationId);
    });
  }

  @Override
  public Set<DatasetId> getDatasets(final ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      return usageTable.getDatasets(id);
    });
  }

  @Override
  public Set<DatasetId> getDatasets(final ProgramId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      return usageTable.getDatasets(id);
    });
  }

  @Override
  public Set<ProgramId> getPrograms(final DatasetId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      return usageTable.getPrograms(id);
    });
  }
}
