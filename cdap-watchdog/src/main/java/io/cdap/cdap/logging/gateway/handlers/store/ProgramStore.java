/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers.store;

import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.MultiThreadDatasetCache;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.store.RunRecordMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

/**
 * This is to for log handler to access run records. Log handler cannot use Store directly because watchdog module
 * doesn't have dependency on app-fabric.
 */
public class ProgramStore {

  private final TransactionRunner transactionRunner;

  @Inject
  public ProgramStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Returns run record for a given run.
   *
   * @param programRunId program run id
   * @return run record for runid
   */
  public RunRecordMeta getRun(ProgramRunId programRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.AppMetadataStore.RUN_RECORDS);
      AppMetadataStore metaStore = new AppMetadataStore(table);
      return metaStore.getRun(programRunId.getParent(), programRunId.getRun());
    });
  }
}
