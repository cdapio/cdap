/*
 * Copyright © 2015-2022 Cask Data, Inc.
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

import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Duplicate store class for application meatadata. JIRA https://cdap.atlassian.net/browse/CDAP-2172
 */
public class AppMetadataStore {

  private static final Gson GSON = new Gson();
  private static final String TYPE_RUN_RECORD_ACTIVE = "runRecordActive";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";

  private final StructuredTable runRecordsTable;

  public AppMetadataStore(StructuredTable table) {
    this.runRecordsTable = table;
  }

  // TODO: getRun is duplicated from cdap-app-fabric AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://cdap.atlassian.net/browse/CDAP-2172
  public RunRecordDetail getRun(ProgramReference programRef, String runId) throws IOException {
    // Query active run record first
    RunRecordDetail running = getUnfinishedRun(programRef, runId);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }
    // If program is not running, query completed run records
    return getCompletedRun(programRef, runId);
  }


  private RunRecordDetail getUnfinishedRun(ProgramReference programRef, String runId)
      throws IOException {
    List<Field<?>> runningKeys = new ArrayList<>();
    runningKeys.add(
        Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, TYPE_RUN_RECORD_ACTIVE));
    addProgramRunReferenceKeys(runningKeys, programRef, runId);
    return getRunRecordMeta(runningKeys);
  }

  private RunRecordDetail getCompletedRun(ProgramReference programRef, String runId)
      throws IOException {
    List<Field<?>> completedKeys = new ArrayList<>();
    completedKeys.add(
        Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, TYPE_RUN_RECORD_COMPLETED));
    addProgramRunReferenceKeys(completedKeys, programRef, runId);
    return getRunRecordMeta(completedKeys);
  }

  private void addProgramRunReferenceKeys(List<Field<?>> fields, ProgramReference programRef,
      String runId) {
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD,
        programRef.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD,
        programRef.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD,
        programRef.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD,
        programRef.getProgram()));
    fields.add(Fields.longField(
        StoreDefinition.AppMetadataStore.RUN_START_TIME,
        getInvertedTsKeyPart(RunIds.getTime(runId, TimeUnit.SECONDS))));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, runId));
  }

  @Nullable
  private RunRecordDetail getRunRecordMeta(List<Field<?>> partialPrimaryKeys) throws IOException {
    // Instead of reading by full primary keys, we scan by all keys without version
    // Since we made sure run id is unique
    try (CloseableIterator<StructuredRow> iterator =
        runRecordsTable.scan(Range.singleton(partialPrimaryKeys), 1)) {
      if (iterator.hasNext()) {
        return deserializeRunRecordMeta(iterator.next());
      }
    }
    return null;
  }

  private static RunRecordDetail deserializeRunRecordMeta(StructuredRow row) {
    RunRecordDetail existing =
        GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA),
            RunRecordDetail.class);
    return RunRecordDetail.builder(existing)
        .setProgramRunId(
            getProgramIdFromRunRecordsPrimaryKeys(new ArrayList(row.getPrimaryKeys())).run(
                existing.getPid()))
        .build();
  }

  private static ProgramId getProgramIdFromRunRecordsPrimaryKeys(List<Field<?>> primaryKeys) {
    // Assume keys are in correct ordering - skip first field since it's run_status
    return new ApplicationId(getStringFromField(primaryKeys.get(1)),
        getStringFromField(primaryKeys.get(2)),
        getStringFromField(primaryKeys.get(3)))
        .program(ProgramType.valueOf(getStringFromField(primaryKeys.get(4))),
            getStringFromField(primaryKeys.get(5)));
  }

  private static String getStringFromField(Field<?> field) {
    return (String) field.getValue();
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }
}
