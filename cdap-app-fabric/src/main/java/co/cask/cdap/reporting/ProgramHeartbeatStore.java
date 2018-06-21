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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Heartbeat Store that writes heart beat messages and program status messages
 * to {@linkConstants.ProgramHeartbeatStore.TABLE}. This is used by the program-report system app to efficiently
 * scan and return results for dashboard status queries
 */
public class ProgramHeartbeatStore extends AbstractDataset {
  private static final Gson GSON = new GsonBuilder().create();
  private static final byte[] COLUMN_NAME = Bytes.toBytes("status");

  static final DatasetId PROGRAM_HEARTBEAT_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeat.TABLE);
  static final byte[] ROW_KEY_SEPARATOR = Bytes.toBytes(":");

  private final Table table;
  private final CConfiguration cConfiguration;

  public ProgramHeartbeatStore(Table table, CConfiguration cConfiguration) {
    super("ignored", table);
    this.table = table;
    this.cConfiguration = cConfiguration;
  }

  /**
   * Static method for creating an instance of {@link ProgramHeartbeatStore}.
   */
  public static ProgramHeartbeatStore create(CConfiguration cConf,
                                             DatasetContext datasetContext,
                                             DatasetFramework datasetFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, PROGRAM_HEARTBEAT_INSTANCE_ID,
                                                    Table.class.getName(), DatasetProperties.EMPTY);
      return new ProgramHeartbeatStore(table, cConf);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * TODO update javadoc
   * create rowkey based on notification and write the program run id with the rowkey
   *
   * @param notification Row key design:
   */
  public void writeProgramHeartBeatStatus(Notification notification) {
    Map<String, String> properties = notification.getProperties();
    long timestamp = Long.parseLong(properties.get(ProgramOptionConstants.HEART_BEAT_TIME));
    table.put(createRowKey(timestamp, properties), COLUMN_NAME, Bytes.toBytes(GSON.toJson(notification)));
  }

  private byte[] createRowKey(long timestamp, Map<String, String> properties) {
    ProgramRunId programRunId = GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_RUN_ID),
                                              ProgramRunId.class);
    // TODO should we just use RunId or the entire programRunId
    return Bytes.add(Bytes.toBytes(timestamp), ROW_KEY_SEPARATOR, Bytes.toBytes(programRunId.toString()));
  }

  /**
   * find timestamp, timestamp key changes based on the program status and
   * create rowkey timestamp:program-status:programRunId. perform a put of notification object with the created rowkey.
   *
   * @param notification
   */
  public void writeProgramStatus(Notification notification) {
    String programStatus = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
    ProgramRunStatus programRunStatus = ProgramRunStatus.valueOf(programStatus);
    if (programRunStatus.equals(ProgramRunStatus.STARTING)) {
      return;
    }
    String timestamp = parseTimestamp(ProgramRunStatus.valueOf(programStatus), notification.getProperties());
    if (timestamp == null) {
      // TODO log an error/warning this shouldn't happen
      return;
    }
    table.put(createRowKey(Long.parseLong(timestamp), notification.getProperties()),
              COLUMN_NAME, Bytes.toBytes(GSON.toJson(notification)));
  }

  /**
   * find the timestamp based on program status and return the string representation of the timestamp.
   */
  private String parseTimestamp(ProgramRunStatus programStatus, Map<String, String> properties) {
    switch (programStatus) {
      case RUNNING:
        return properties.get(ProgramOptionConstants.LOGICAL_START_TIME);
      case SUSPENDED:
        return properties.get(ProgramOptionConstants.SUSPEND_TIME);
      case COMPLETED:
      case KILLED:
      case FAILED:
        return properties.get(ProgramOptionConstants.END_TIME);
      case RESUMING:
        return properties.get(ProgramOptionConstants.RESUME_TIME);
      default:
        return null;
    }
  }

  // TODO remove this - just used for debugging
 /* void printAll() {
    try (Scanner scanner = table.scan(null, null)) {
      Row row;
      List<Notification> result = new ArrayList<>();
      Map<ProgramRunId, Notification> runningRuns = new HashMap();
      while ((row = scanner.next()) != null) {
        Notification notification = GSON.fromJson(Bytes.toString(row.getColumns().get(COLUMN_NAME)),
                                                  Notification.class);
        Map<String, String> properties = notification.getProperties();
        // Required parameters
        ProgramRunId programRunId = GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_RUN_ID),
                                                  ProgramRunId.class);
        String time = properties.get(ProgramOptionConstants.HEART_BEAT_TIME);
        System.out.println(String.format(" Program runId %s Heartbeat time %s",
                                         programRunId.toString(), new Date(Long.parseLong(time)).toString()));
      }
    }
  }*/

  /**
   * scan the table for the time range and return collection of completed and actively running runs
   *
   * @return collection of program runid matching the parameter requirements
   */
  Collection<Notification> scan ( byte[] startRowKey, byte[] endRowKey) {
    try (Scanner scanner = table.scan(startRowKey, endRowKey)) {
      Row row;
      Map<ProgramRunId, Notification> result = new HashMap<>();
      while ((row = scanner.next()) != null) {
        Notification notification = GSON.fromJson(Bytes.toString(row.getColumns().get(COLUMN_NAME)),
                                                  Notification.class);
        Map<String, String> properties = notification.getProperties();
        // Required parameters
        ProgramRunId programRunId = GSON.fromJson(properties.get(ProgramOptionConstants.PROGRAM_RUN_ID),
                                                  ProgramRunId.class);
        ProgramRunStatus programRunStatus =
          ProgramRunStatus.valueOf(properties.get(ProgramOptionConstants.PROGRAM_STATUS));
        if (programRunStatus.equals(ProgramRunStatus.STARTING)) {
          // this shouldn't happen as we skip writing starting status runs.
          continue;
        }
        // this ensures we return the most recent status of the program for this time range.
        result.put(programRunId, notification);
      }
      return result.values();
    }
  }
}

