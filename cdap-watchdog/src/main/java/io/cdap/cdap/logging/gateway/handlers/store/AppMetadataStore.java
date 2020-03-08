/*
 * Copyright © 2015-2020 Cask Data, Inc.
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
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Duplicate store class for application meatadata.
 * JIRA https://issues.cask.co/browse/CDAP-2172
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
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecordDetail getRun(ProgramId programId, String runId) throws IOException {
    // Query active run record first
    RunRecordDetail running = getUnfinishedRun(programId, runId);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }
    // If program is not running, query completed run records
    return getCompletedRun(programId, runId);
  }


  private RunRecordDetail getUnfinishedRun(ProgramId programId, String runId) throws IOException {
    List<Field<?>> runningKey = getRunRecordProgramPrefix(TYPE_RUN_RECORD_ACTIVE, programId);
    runningKey.add(Fields.longField(
      StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(RunIds.getTime(runId, TimeUnit.SECONDS))));
    runningKey.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, runId));
    return getRunRecordMeta(runningKey);
  }

  private RunRecordDetail getRunRecordMeta(List<Field<?>> primaryKeys) throws IOException {
    Optional<StructuredRow> row = runRecordsTable.read(primaryKeys);
    if (!row.isPresent()) {
      return null;
    }
    return deserializeRunRecordMeta(row.get());
  }

  private static RunRecordDetail deserializeRunRecordMeta(StructuredRow row) {
    RunRecordDetail existing =
      GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordDetail.class);
    RunRecordDetail newMeta =
      RunRecordDetail.builder(existing)
        .setProgramRunId(
          getProgramIdFromRunRecordsPrimaryKeys(new ArrayList(row.getPrimaryKeys())).run(existing.getPid()))
        .build();
    return newMeta;
  }

  private static ProgramId getProgramIdFromRunRecordsPrimaryKeys(List<Field<?>> primaryKeys) {
    // Assume keys are in correct ordering - skip first field since it's run_status
    return new ApplicationId(getStringFromField(primaryKeys.get(1)), getStringFromField(primaryKeys.get(2)),
                             getStringFromField(primaryKeys.get(3)))
      .program(ProgramType.valueOf(getStringFromField(primaryKeys.get(4))), getStringFromField(primaryKeys.get(5)));
  }

  private static String getStringFromField(Field<?> field) {
    return (String) field.getValue();
  }


  private RunRecordDetail getCompletedRun(ProgramId programId, final String runId)
    throws IOException {
    List<Field<?>> completedKey = getRunRecordProgramPrefix(TYPE_RUN_RECORD_COMPLETED, programId);
    // Get start time from RunId
    long programStartSecs = RunIds.getTime(RunIds.fromString(runId), TimeUnit.SECONDS);
    completedKey.add(
      Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(programStartSecs)));
    completedKey.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, runId));
    return getRunRecordMeta(completedKey);
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  private List<Field<?>> getRunRecordProgramPrefix(String status, @Nullable ProgramId programId) {
    if (programId == null) {
      return getRunRecordStatusPrefix(status);
    }
    List<Field<?>> fields =
      getRunRecordApplicationPrefix(
        status, new ApplicationId(programId.getNamespace(), programId.getApplication(), programId.getVersion()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD, programId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD, programId.getProgram()));
    return fields;
  }

  private List<Field<?>> getRunRecordStatusPrefix(String status) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, status));
    return fields;
  }

  private List<Field<?>> getRunRecordApplicationPrefix(String status, @Nullable ApplicationId applicationId) {
    List<Field<?>> fields = getRunRecordStatusPrefix(status);
    if (applicationId == null) {
      return fields;
    }
    fields.addAll(getApplicationPrimaryKeys(
      applicationId.getNamespace(), applicationId.getApplication(), applicationId.getVersion()));
    return fields;
  }

  private List<Field<?>> getApplicationPrimaryKeys(String namespaceId, String appId, String versionId) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, appId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, versionId));
    return fields;
  }
}
