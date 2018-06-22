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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProgramHeartBeatDatasetTest {
  private static DatasetFramework datasetFramework;
  private static CConfiguration cConf;
  private static TransactionExecutorFactory txExecutorFactory;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    datasetFramework = injector.getInstance(DatasetFramework.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    cConf = injector.getInstance(CConfiguration.class);
  }

  @Test
  public void testWritingScanningHeartBeats() throws Exception {
    ProgramHeartbeatDataset programHeartbeatDataset = getHeartBeatStore("testBeats");
    TransactionExecutor txnl = getTxExecutor(programHeartbeatDataset);

    //  write program status "running" for program 1 starting at x
    long startTime1 = System.currentTimeMillis();
    Map<String, String> properties = getMockProgramProperties(RunIds.generate(), startTime1);
    Notification run1Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    txnl.execute(() -> {
      programHeartbeatDataset.writeProgramStatus(run1Notification);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toMillis(10);
    long interval = TimeUnit.MINUTES.toMillis(1);
    setUpProgramHeartBeats(properties, startTime1, endTime, interval, txnl, programHeartbeatDataset);


    //  write program status "running" for program 2 starting at x + 5
    long startTime2 = startTime1 + TimeUnit.MINUTES.toMillis(5);
    properties = getMockProgramProperties(RunIds.generate(), startTime2);
    Notification run2Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    txnl.execute(() -> {
      programHeartbeatDataset.writeProgramStatus(run2Notification);
    });

    // write heart beat messages for 5 minutes (every minute) for this program run.
    setUpProgramHeartBeats(properties, startTime2, endTime, interval, txnl, programHeartbeatDataset);

    // program run1 runtime -> x     : x + 10
    // program run2 runtime -> x + 5 : x + 10
    txnl.execute(() -> {
      byte[] startRowKey1 = Bytes.toBytes(startTime1);
      byte[] startRowKey2 = Bytes.toBytes(startTime2);
      byte[] endRowKey = Bytes.toBytes(endTime);
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Assert.assertEquals(1 , programHeartbeatDataset.scan(startRowKey1, startRowKey2).size());
      // scanning from x : x + 10, should return both
      Assert.assertEquals(2 , programHeartbeatDataset.scan(startRowKey1, endRowKey).size());
      // scanning from x + 5 : x + 10, should return both
      Assert.assertEquals(2 , programHeartbeatDataset.scan(startRowKey2, endRowKey).size());
    });
  }

  /**
   * writes heart beat messages starting from startTime + interval up to endTime, each heartbeat separated by interval
   */
  private void setUpProgramHeartBeats(Map<String, String> properties,
                                      long startTime, long endTime, long interval,
                                      TransactionExecutor txnl, ProgramHeartbeatDataset programHeartbeatDataset)
    throws InterruptedException, TransactionFailureException {
    for (long time = startTime + interval; time <= endTime; time += interval) {
      properties.put(ProgramOptionConstants.HEART_BEAT_TIME, String.valueOf(time));
      Notification heartbeat = new Notification(Notification.Type.HEART_BEAT, properties);
      txnl.execute(() -> {
        programHeartbeatDataset.writeProgramHeartBeatStatus(heartbeat);
      });
    }
  }

  /**
   * setup and return mock program properties with status running, runId and time when it started running are
   * specified by parameters
   */
  private Map<String, String> getMockProgramProperties(RunId runId, long runningTime) {
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, ARTIFACT_ID.toString());
    systemArguments.put(ProgramOptionConstants.PRINCIPAL, "userA");
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());

    Map<String, String> properties = new HashMap<>();

    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programId.run(runId)));
    properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(runningTime));
    properties.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
    properties.put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions));
    return properties;
  }

  @Test
  public void testScanningProgramStatus() throws Exception {
    // p1 : x           ->    x + 5
    // scanning from x -> x + 4 -> return one record with status running
    // scanning from x -> x + 5min + 1ms -> return one record with status killed

    ProgramHeartbeatDataset programHeartbeatDataset = getHeartBeatStore("testStatusChange");
    TransactionExecutor txnl = getTxExecutor(programHeartbeatDataset);

    //  write program status "running" for program 1 starting at x
    long startTime1 = System.currentTimeMillis();
    Map<String, String> properties = getMockProgramProperties(RunIds.generate(), startTime1);
    Notification run1Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    txnl.execute(() -> {
      programHeartbeatDataset.writeProgramStatus(run1Notification);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long heartBeatEndTime = startTime1 + TimeUnit.MINUTES.toMillis(4);
    long interval = TimeUnit.MINUTES.toMillis(1);
    setUpProgramHeartBeats(properties, startTime1, heartBeatEndTime, interval, txnl, programHeartbeatDataset);

    long programEndTime = startTime1 + TimeUnit.MINUTES.toMillis(5);

    // update status to Killed
    properties.put(ProgramOptionConstants.END_TIME, String.valueOf(programEndTime));
    properties.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.KILLED.name());
    Notification run1End = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    txnl.execute(() -> {
      programHeartbeatDataset.writeProgramStatus(run1End);
    });

    // perform scan checks
    txnl.execute(() -> {
      byte[] startRowKey1 = Bytes.toBytes(startTime1);
      byte[] stillRunning = Bytes.toBytes(heartBeatEndTime);
      byte[] endRowKey = Bytes.toBytes(programEndTime + 1);
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Collection<Notification> notificationCollection = programHeartbeatDataset.scan(startRowKey1, stillRunning);
      Assert.assertEquals(1 , notificationCollection.size());
      Assert.assertEquals(ProgramRunStatus.RUNNING.name(),
                          notificationCollection.iterator().next().
                            getProperties().get(ProgramOptionConstants.PROGRAM_STATUS));
      // scanning from x : x + (5 min + 1ms), should return run1 with status KILLED
      notificationCollection = programHeartbeatDataset.scan(startRowKey1, endRowKey);
      Assert.assertEquals(1 , notificationCollection.size());
      Assert.assertEquals(ProgramRunStatus.KILLED.name(),
                          notificationCollection.iterator().next().
                            getProperties().get(ProgramOptionConstants.PROGRAM_STATUS));
    });
  }

  // TODO generate running for 100 runs with 50 going into different completed status
  @Test
  public void testScanningMultipleRuns() throws Exception {

  }

  private TransactionExecutor getTxExecutor(ProgramHeartbeatDataset programHeartbeatDataset) {
    return txExecutorFactory.createExecutor(Collections.singleton(programHeartbeatDataset));
  }

  private ProgramHeartbeatDataset getHeartBeatStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.SYSTEM.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);
    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.of(), null);
    Assert.assertNotNull(table);
    return new ProgramHeartbeatDataset(table);
  }
}
