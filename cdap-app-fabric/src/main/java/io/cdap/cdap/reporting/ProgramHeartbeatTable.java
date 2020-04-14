/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.reporting;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Heartbeat Store that writes heart beat messages and program status messages
 * to program heartbeat table. This is used for efficiently
 * scanning and returning results for dashboard status queries.
 */
public class ProgramHeartbeatTable {
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private final StructuredTable table;

  // TODO: CDAP-14950 add service to clean up this table periodically
  public ProgramHeartbeatTable(StructuredTableContext context) {
    this.table = context.getTable(StoreDefinition.ProgramHeartbeatStore.PROGRAM_HEARTBEATS);
  }

  /**
   * Write {@link RunRecordDetail} to heart beat table as value.
   *
   * @param runRecordMeta row value to write
   * @param timestampInSeconds used for creating rowKey
   */
  public void writeRunRecordMeta(RunRecordDetail runRecordMeta, long timestampInSeconds) throws IOException {
    List<Field<?>> fields = createRowKey(timestampInSeconds, runRecordMeta.getProgramRunId());
    fields.add(Fields.stringField(StoreDefinition.ProgramHeartbeatStore.RUN_RECORD, GSON.toJson(runRecordMeta)));
    table.upsert(fields);
  }

  @VisibleForTesting
  public void deleteAll() throws IOException {
    table.deleteAll(Range.all());
  }

  private List<Field<?>> createRowKey(long timestampInSeconds, ProgramRunId programRunId) {
    List<Field<?>> fields = new ArrayList<>();
    // add namespace at the beginning
    fields.add(Fields.stringField(StoreDefinition.ProgramHeartbeatStore.NAMESPACE_FIELD, programRunId.getNamespace()));
    // add timestamp
    fields.add(Fields.longField(StoreDefinition.ProgramHeartbeatStore.TIMESTAMP_SECONDS_FIELD, timestampInSeconds));
    // add program runId fields, skip namespace as that is part of row key
    fields.add(
      Fields.stringField(StoreDefinition.ProgramHeartbeatStore.APPLICATION_FIELD, programRunId.getApplication()));
    fields.add(
      Fields.stringField(StoreDefinition.ProgramHeartbeatStore.PROGRAM_TYPE_FIELD, programRunId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.ProgramHeartbeatStore.PROGRAM_FIELD, programRunId.getProgram()));
    fields.add(Fields.stringField(StoreDefinition.ProgramHeartbeatStore.RUN_FIELD, programRunId.getRun()));
    return fields;
  }

  /**
   * Add namespace and timestamp and return it as the scan key.
   * @return scan key
   */
  private List<Field<?>> getScanKey(String namespace, long timestamp) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.ProgramHeartbeatStore.NAMESPACE_FIELD, namespace));
    fields.add(Fields.longField(StoreDefinition.ProgramHeartbeatStore.TIMESTAMP_SECONDS_FIELD, timestamp));
    return fields;
  }

  /**
   * Scan the table for the time range for each of the namespace provided
   * and return collection of latest {@link RunRecordDetail}
   * we maintain the latest {@link RunRecordDetail} identified by {@link ProgramRunId},
   * Since there can be more than one RunRecordDetail for the
   * same runId due to multiple state changes and heart beat messages.
   *
   * @param startTimestampInSeconds inclusive start rowKey
   * @param endTimestampInSeconds exclusive end rowKey
   * @param namespaces set of namespaces
   * @return collection of {@link RunRecordDetail}
   */
  public Collection<RunRecordDetail> scan(long startTimestampInSeconds, long endTimestampInSeconds,
                                          Set<String> namespaces) throws IOException {
    List<RunRecordDetail> resultRunRecordList = new ArrayList<>();
    for (String namespace : namespaces) {
      List<Field<?>> startRowKey = getScanKey(namespace, startTimestampInSeconds);
      List<Field<?>> endRowKey = getScanKey(namespace, endTimestampInSeconds);
      performScanAddToList(startRowKey, endRowKey, resultRunRecordList);
    }
    return resultRunRecordList;
  }

  /**
   * Scan is executed based on the given startRowKey and endRowKey, for each of the scanned rows, we maintain
   * the latest {@link RunRecordDetail} identified by its {@link ProgramRunId} in a map. Finally after scan is
   * complete add the runrecords to the result list
   *
   * @param startRowKey byte array used as start row key in scan
   * @param endRowKey byte array used as end row key in scan
   * @param runRecordMetas result list to which the run records to be added
   */
  private void performScanAddToList(List<Field<?>> startRowKey, List<Field<?>> endRowKey,
                                    List<RunRecordDetail> runRecordMetas) throws IOException {
    Map<ProgramRunId, RunRecordDetail> latestRunRecords = new LinkedHashMap<>();;
    try (CloseableIterator<StructuredRow> iterator =
      table.scan(Range.create(startRowKey, Range.Bound.INCLUSIVE, endRowKey, Range.Bound.EXCLUSIVE),
                 Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        RunRecordDetail existing = GSON.fromJson(row.getString(StoreDefinition.ProgramHeartbeatStore.RUN_RECORD),
                                                 RunRecordDetail.class);
        ProgramRunId runId = getProgramRunIdFromRow(row);
        latestRunRecords.put(runId, RunRecordDetail.builder(existing).setProgramRunId(runId).build());
      }
    }
    runRecordMetas.addAll(latestRunRecords.values());
  }

  /**
   * Return {@link ProgramRunId} from the row
   */
  private ProgramRunId getProgramRunIdFromRow(StructuredRow row) {
    return new ProgramRunId(row.getString(StoreDefinition.ProgramHeartbeatStore.NAMESPACE_FIELD),
                            row.getString(StoreDefinition.ProgramHeartbeatStore.APPLICATION_FIELD),
                            ProgramType.valueOf(
                              row.getString(StoreDefinition.ProgramHeartbeatStore.PROGRAM_TYPE_FIELD)),
                            row.getString(StoreDefinition.ProgramHeartbeatStore.PROGRAM_FIELD),
                            row.getString(StoreDefinition.ProgramHeartbeatStore.RUN_FIELD));
  }
}
