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

import com.google.inject.Inject;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

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
  public RunRecordDetail getRun(ProgramRunId programRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable table = context.getTable(StoreDefinition.AppMetadataStore.RUN_RECORDS);
      AppMetadataStore metaStore = new AppMetadataStore(table);
      return metaStore.getRun(programRunId.getParent(), programRunId.getRun());
    });
  }
}
