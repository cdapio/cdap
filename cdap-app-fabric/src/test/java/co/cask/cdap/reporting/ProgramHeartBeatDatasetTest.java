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
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ProgramHeartBeatDatasetTest {
  private static DatasetFramework datasetFramework;
  private static TransactionExecutorFactory txExecutorFactory;
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final byte[] SOURCE_ID = Bytes.toBytes("sourceId");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    datasetFramework = injector.getInstance(DatasetFramework.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  @Test
  public void testWritingScanningHeartBeats() throws Exception {
    ProgramHeartbeatDataset programHeartbeatDataset = getHeartBeatStore("testBeats");
    TransactionExecutor txnl = txExecutorFactory.createExecutor(Collections.singleton(programHeartbeatDataset));

    //  write program status "running" for program 1 starting at x
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId = RunIds.generate();
    RunRecordMeta.Builder metaBuilder =
      getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaBuilder.setRunTime(startTime1);
    metaBuilder.setStatus(ProgramRunStatus.RUNNING);
    RunRecordMeta meta = metaBuilder.build();
    txnl.execute(() -> {
      programHeartbeatDataset.writeRunRecordMeta(meta, startTime1);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toSeconds(10);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(meta, startTime1, endTime, interval, txnl, programHeartbeatDataset);


    //  write program status "running" for program 2 starting at x + 5
    long startTime2 = startTime1 + TimeUnit.MINUTES.toSeconds(5);
    RunId runId2 = RunIds.generate();
    RunRecordMeta.Builder metaBuilder2 = getMockRunRecordMeta(NamespaceId.DEFAULT, runId2);
    metaBuilder2.setRunTime(startTime2);
    metaBuilder2.setStatus(ProgramRunStatus.RUNNING);
    RunRecordMeta meta2 = metaBuilder2.build();
    txnl.execute(() -> {
      programHeartbeatDataset.writeRunRecordMeta(meta2, startTime2);
    });

    // write heart beat messages for 5 minutes (every minute) for this program run.
    setUpProgramHeartBeats(meta2, startTime2, endTime, interval, txnl, programHeartbeatDataset);

    // program run1 runtime -> x     : x + 10
    // program run2 runtime -> x + 5 : x + 10
    txnl.execute(() -> {
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Collection<RunRecordMeta> result =
        programHeartbeatDataset.scan(startTime1, startTime2, ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));

      Assert.assertEquals(1 , result.size());
      Assert.assertEquals(meta, result.iterator().next());
      // scanning from x : x + 10, should return both
      Set<RunRecordMeta> expected = ImmutableSet.of(meta, meta2);
      result = programHeartbeatDataset.scan(startTime1, endTime,
                                            ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(expected.size() , result.size());
      Assert.assertTrue(result.containsAll(expected));

      // scanning from x + 5 : x + 10, should return both
      result = programHeartbeatDataset.scan(startTime2, endTime,
                                            ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(expected.size() , result.size());
      Assert.assertTrue(result.containsAll(expected));
    });
  }

  /**
   * setup and return mock program properties on runrecord builder but use passed namespaceId and runId
   */
  private RunRecordMeta.Builder getMockRunRecordMeta(NamespaceId namespaceId, RunId runId) {
    ProgramId programId = namespaceId.app("someapp").program(ProgramType.SERVICE, "s");
    RunRecordMeta.Builder runRecordBuilder = RunRecordMeta.builder();
    runRecordBuilder.setArtifactId(ARTIFACT_ID);
    runRecordBuilder.setPrincipal("userA");
    runRecordBuilder.setProgramRunId(programId.run(runId));
    runRecordBuilder.setSourceId(SOURCE_ID);
    runRecordBuilder.setStartTime(RunIds.getTime(runId, TimeUnit.SECONDS));
    return runRecordBuilder;
  }

  /**
   * writes heart beat messages starting from startTime + interval up to endTime, each heartbeat separated by interval
   */
  private void setUpProgramHeartBeats(RunRecordMeta runRecordMeta,
                                      long startTime, long endTime, long interval,
                                      TransactionExecutor txnl, ProgramHeartbeatDataset programHeartbeatDataset)
    throws InterruptedException, TransactionFailureException {
    txnl.execute(() -> {
      for (long time = startTime + interval; time < endTime; time += interval) {
        programHeartbeatDataset.writeRunRecordMeta(runRecordMeta, time);
      }
    });
  }

  @Test
  public void testScanningProgramStatus() throws Exception {
    // p1 : x           ->    x + 5
    // scanning from x -> x + 4 -> return one record with status running
    // scanning from x -> x + 5min + 1s -> return one record with status killed

    ProgramHeartbeatDataset programHeartbeatDataset = getHeartBeatStore("testStatusChange");
    TransactionExecutor txnl = txExecutorFactory.createExecutor(Collections.singleton(programHeartbeatDataset));

    //  write program status "running" for program 1 starting at x
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId = RunIds.generate();
    RunRecordMeta.Builder metaRunningBuilder = getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaRunningBuilder.setStatus(ProgramRunStatus.RUNNING);
    metaRunningBuilder.setRunTime(startTime1);
    RunRecordMeta metaRunning = metaRunningBuilder.build();
    txnl.execute(() -> {
      programHeartbeatDataset.writeRunRecordMeta(metaRunning, startTime1);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long heartBeatEndTime = startTime1 + TimeUnit.MINUTES.toSeconds(4);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(metaRunning, startTime1, heartBeatEndTime, interval, txnl, programHeartbeatDataset);

    long programEndTime = startTime1 + TimeUnit.MINUTES.toSeconds(5);

    RunRecordMeta.Builder metaKilledBuilder = getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaKilledBuilder.setStatus(ProgramRunStatus.KILLED);
    metaKilledBuilder.setStopTime(programEndTime);
    RunRecordMeta metaKilled = metaKilledBuilder.build();
    txnl.execute(() -> {
      programHeartbeatDataset.writeRunRecordMeta(metaKilled, programEndTime);
    });

    // perform scan checks
    txnl.execute(() -> {
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Collection<RunRecordMeta> runRecordMetaList =
        programHeartbeatDataset.scan(startTime1, heartBeatEndTime, ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(1 , runRecordMetaList.size());
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMetaList.iterator().next().getStatus());
      Assert.assertEquals(metaRunning, runRecordMetaList.iterator().next());
      // scanning from x : x + (5 min + 1s), should return run1 with status KILLED
      runRecordMetaList = programHeartbeatDataset.scan(startTime1, programEndTime + 1,
                                                       ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(1 , runRecordMetaList.size());
      Assert.assertEquals(metaKilled, runRecordMetaList.iterator().next());
    });
  }

  private ProgramHeartbeatDataset getHeartBeatStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.SYSTEM.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);
    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.of(), null);
    Assert.assertNotNull(table);
    return new ProgramHeartbeatDataset(table);
  }
}
