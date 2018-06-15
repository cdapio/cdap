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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.Map;

public class ProgramHeartbeatStore {
  private static final Gson GSON = new GsonBuilder().create();
  private static final byte[] COLUMN_NAME = Bytes.toBytes("status");

  static final DatasetId PROGRAM_HEARTBEAT_INSTANCE_ID =
    NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeatStore.TABLE);
  static final String ROW_KEY_SEPARATOR = ":";

  private final Table table;
  private final CConfiguration cConfiguration;

  ProgramHeartbeatStore(Table table, CConfiguration cConfiguration) {
    this.table = table;
    this.cConfiguration = cConfiguration;
  }

  /**
   * Static method for creating an instance of {@link AppMetadataStore}.
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
   *
   * TODO update javadoc
   * create rowkey based on notification and write the program run id with the rowkey
   * @param notification
   * @param programRunId
   * Row key design:
   *
   * <message-timestamp>:<program-status>:<program-run-id>
   *
   * where:
   *
   * <message-timestamp>: the time when the message is published
   *
   *
   * <program-status>: program status - we also publish status during start, stop, error, etc along with heartbeat during running  (optional ?)
   *
   * <program-run-id>: app+program type+program+run
   *
   * Value:
   *
   * Map of <String, String> or object including ProgramRunId, system arguments, ArtifactId, principal, startTime, runningTime, stopTime.
   */
  public void writeProgramHeartBeatStatus(Notification notification, ProgramRunId programRunId) {
    String timestamp = notification.getProperties().get(ProgramOptionConstants.HEART_BEAT_TIME);
    String programStatus = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
    byte[] rowKey = createRowKey(timestamp, programStatus, programRunId);
    table.put(rowKey, COLUMN_NAME, Bytes.toBytes(GSON.toJson(notification)));
  }

  private byte[] createRowKey(String timestamp, String programStatus, ProgramRunId programRunId) {
    String rowKey = String.format("%s%s%s%s%s",
                                  timestamp, ROW_KEY_SEPARATOR, programStatus, ROW_KEY_SEPARATOR,
                                  GSON.toJson(programRunId));

    return Bytes.toBytes(rowKey);
  }


  public void writeProgramStatus(Notification notification, ProgramRunId programRunId) {
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
    table.put(createRowKey(timestamp, programStatus, programRunId), COLUMN_NAME,
              Bytes.toBytes(GSON.toJson(notification)));

  }

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

}
