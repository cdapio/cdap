/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.reporting;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Heartbeat Store that writes heart beat messages and program status messages
 * to program heartbeat table. This is used for efficiently
 * scanning and returning results for dashboard status queries
 *
 * Rowkey for the table is organized in the following format
 * [namespace][timestamp-in-seconds][program-run-Id]
 *
 */
public class ProgramHeartbeatDataset extends AbstractDataset {
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final byte[] COLUMN_NAME = Bytes.toBytes("status");
  private static final DatasetId PROGRAM_HEARTBEAT_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeat.TABLE);
  private final Table table;

  public ProgramHeartbeatDataset(Table table) {
    super("ignored", table);
    this.table = table;
  }

  /**
   * Static method for creating an instance of {@link ProgramHeartbeatDataset}.
   */
  public static ProgramHeartbeatDataset getOrCreate(DatasetContext datasetContext,
                                                    DatasetFramework datasetFramework, CConfiguration cConfiguration) {
    try {
      TableProperties.Builder props = TableProperties.builder();
      long ttl = cConfiguration.getLong(Constants.ProgramHeartbeat.HEARTBEAT_TABLE_TTL_SECONDS);
      props.setTTL(ttl);
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, PROGRAM_HEARTBEAT_INSTANCE_ID,
                                                    Table.class.getName(), props.build());
      return new ProgramHeartbeatDataset(table);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write {@link RunRecordMeta} to heart beat dataset as value,
   * with rowKey in format [namespace][timestamp-in-seconds][program-run-Id]
   *
   * @param runRecordMeta row value to write
   * @param timestampInSeconds used for creating rowKey
   */
  public void writeRunRecordMeta(RunRecordMeta runRecordMeta, long timestampInSeconds) {
    byte[] rowKey = createRowKey(timestampInSeconds, runRecordMeta.getProgramRunId());
    table.put(rowKey, COLUMN_NAME, Bytes.toBytes(GSON.toJson(runRecordMeta)));
  }

  private byte[] createRowKey(long timestampInSeconds, ProgramRunId programRunId) {
    MDSKey.Builder builder = new MDSKey.Builder();
    // add namespace at the beginning
    builder.add(programRunId.getNamespace());
    // add timestamp
    builder.add(timestampInSeconds);
    // add program runId fields, skip namespace as that is part of row key
    builder.add(programRunId.getApplication());
    builder.add(programRunId.getType().name());
    builder.add(programRunId.getProgram());
    builder.add(programRunId.getRun());

    return builder.build().getKey();
  }

  /**
   * Add namespace and timestamp and return it as the scan key
   * @return scan key
   */
  private byte[] getScanKey(String namespace, long timestamp) {
    MDSKey.Builder scanKeyBuilder = new MDSKey.Builder();
    scanKeyBuilder.add(namespace);
    scanKeyBuilder.add(timestamp);
    return scanKeyBuilder.build().getKey();
  }

  /**
   * Scan the table for the time range for each of the namespace provided
   * and return collection of latest {@link RunRecordMeta}
   * we maintain the latest {@link RunRecordMeta} identified by {@link ProgramRunId},
   * Since there can be more than one RunRecordMeta for the
   * same runId due to multiple state changes and heart beat messages.
   *
   * @param startTimestampInSeconds inclusive start rowKey
   * @param endTimestampInSeconds exclusive end rowKey
   * @param namespaces set of namespaces
   * @return collection of {@link RunRecordMeta}
   */
  public Collection<RunRecordMeta> scan(long startTimestampInSeconds, long endTimestampInSeconds,
                                        Set<String> namespaces) {
    List<RunRecordMeta> resultRunRecordList = new ArrayList<>();
    for (String namespace : namespaces) {
      byte[] startRowKey = getScanKey(namespace, startTimestampInSeconds);
      byte[] endRowKey = getScanKey(namespace, endTimestampInSeconds);
      performScanAddToList(startRowKey, endRowKey, resultRunRecordList);
    }
    return resultRunRecordList;
  }

  /**
   * Scan is executed based on the given startRowKey and endRowKey, for each of the scanned rows, we maintain
   * the latest {@link RunRecordMeta} identified by its {@link ProgramRunId} in a map. Finally after scan is
   * complete add the runrecords to the result list
   *
   * @param startRowKey byte array used as start row key in scan
   * @param endRowKey byte array used as end row key in scan
   * @param runRecordMetas result list to which the run records to be added
   */
  private void performScanAddToList(byte[] startRowKey, byte[] endRowKey, List<RunRecordMeta> runRecordMetas) {
    try (Scanner scanner = table.scan(startRowKey, endRowKey)) {
      Row row;
      Map<ProgramRunId, RunRecordMeta> runIdToRunRecordMap = new HashMap<>();
      while ((row = scanner.next()) != null) {
        RunRecordMeta runRecordMeta = GSON.fromJson(Bytes.toString(row.getColumns().get(COLUMN_NAME)),
                                                    RunRecordMeta.class);
        ProgramRunId runId = getProgramRunIdFromRowKey(row.getRow());
        runIdToRunRecordMap.put(runId, runRecordMeta);
      }

      // since the serialized runRecordMeta doesn't have programRunId (transient), we will create and
      // add the programRunId to RunRecordMeta and add to result list

      runIdToRunRecordMap.entrySet().forEach((entry) -> {
        RunRecordMeta.Builder builder = RunRecordMeta.builder(entry.getValue());
        builder.setProgramRunId(entry.getKey());
        runRecordMetas.add(builder.build());
      });
    }
  }

  /**
   * Deserialize and return {@link ProgramRunId} from rowKey
   */
  private ProgramRunId getProgramRunIdFromRowKey(byte[] rowKey) {
    MDSKey.Splitter splitter = new MDSKey(rowKey).split();
    // get namespace
    String namespace = splitter.getString();
    // skip timestamp
    splitter.skipLong();
    // now read the programRunId fields, create and return ProgramRunId
    return new ProgramRunId(namespace, splitter.getString(),
                            ProgramType.valueOf(splitter.getString()),
                            splitter.getString(), splitter.getString());
  }
}
