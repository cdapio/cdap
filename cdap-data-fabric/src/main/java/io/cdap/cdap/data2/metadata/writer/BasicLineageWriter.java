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

package io.cdap.cdap.data2.metadata.writer;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.LineageTable;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageTable;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Basic implementation of {@link LineageWriter} and {@link FieldLineageWriter}.
 * Implementation of LineageWriter write to the {@link LineageTable} where as
 * implementation of FieldLineageWriter writes to the {@link FieldLineageTable} directly.
 */
public class BasicLineageWriter implements LineageWriter, FieldLineageWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLineageWriter.class);

  private final TransactionRunner transactionRunner;

  @VisibleForTesting
  @Inject
  public BasicLineageWriter(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetId, AccessType accessType,
                        @Nullable NamespacedEntityId namespacedEntityId) {
    long accessTime = System.currentTimeMillis();
    LOG.trace("Writing access for run {}, dataset {}, accessType {}, accessTime = {}",
              run, datasetId, accessType, accessTime);

    TransactionRunners.run(transactionRunner, context -> {
      LineageTable
        .create(context)
        .addAccess(run, datasetId, accessType, accessTime);
    });
  }

  @Override
  public void write(ProgramRunId programRunId, FieldLineageInfo info) {
    TransactionRunners.run(transactionRunner, context -> {
      FieldLineageTable fieldLineageTable = FieldLineageTable.create(context);
      fieldLineageTable.addFieldLineageInfo(programRunId, info);
    });
  }
}
