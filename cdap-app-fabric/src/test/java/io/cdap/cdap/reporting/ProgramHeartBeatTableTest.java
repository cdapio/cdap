/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class ProgramHeartBeatTableTest {
  protected static TransactionRunner transactionRunner;
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final byte[] SOURCE_ID = Bytes.toBytes("sourceId");

  @Before
  public void before() {
    TransactionRunners.run(transactionRunner, context -> {
      new ProgramHeartbeatTable(context).deleteAll();
    });
  }

  @Test
  public void testWritingScanningHeartBeats() throws Exception {
    //  write program status "running" for program 1 starting at x
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId = RunIds.generate();
    RunRecordDetail.Builder metaBuilder =
      getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaBuilder.setRunTime(startTime1);
    metaBuilder.setStatus(ProgramRunStatus.RUNNING);
    RunRecordDetail meta = metaBuilder.build();
    TransactionRunners.run(transactionRunner, context -> {
      new ProgramHeartbeatTable(context).writeRunRecordMeta(meta, startTime1);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toSeconds(10);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(meta, startTime1, endTime, interval);


    //  write program status "running" for program 2 starting at x + 5
    long startTime2 = startTime1 + TimeUnit.MINUTES.toSeconds(5);
    RunId runId2 = RunIds.generate();
    RunRecordDetail.Builder metaBuilder2 = getMockRunRecordMeta(NamespaceId.DEFAULT, runId2);
    metaBuilder2.setRunTime(startTime2);
    metaBuilder2.setStatus(ProgramRunStatus.RUNNING);
    RunRecordDetail meta2 = metaBuilder2.build();
    TransactionRunners.run(transactionRunner, context -> {
      new ProgramHeartbeatTable(context).writeRunRecordMeta(meta2, startTime2);
    });

    // write heart beat messages for 5 minutes (every minute) for this program run.
    setUpProgramHeartBeats(meta2, startTime2, endTime, interval);

    // program run1 runtime -> x     : x + 10
    // program run2 runtime -> x + 5 : x + 10
    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Collection<RunRecordDetail> result =
        programHeartbeatTable.scan(startTime1, startTime2, ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));

      Assert.assertEquals(1 , result.size());
      Assert.assertEquals(meta, result.iterator().next());
      // scanning from x : x + 10, should return both
      Set<RunRecordDetail> expected = ImmutableSet.of(meta, meta2);
      result = programHeartbeatTable.scan(startTime1, endTime,
                                          ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(expected.size() , result.size());
      Assert.assertTrue(result.containsAll(expected));

      // scanning from x + 5 : x + 10, should return both
      result = programHeartbeatTable.scan(startTime2, endTime,
                                          ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(expected.size() , result.size());
      Assert.assertTrue(result.containsAll(expected));
    });
  }

  /**
   * setup and return mock program properties on runrecord builder but use passed namespaceId and runId
   */
  private RunRecordDetail.Builder getMockRunRecordMeta(NamespaceId namespaceId, RunId runId) {
    ProgramId programId = namespaceId.app("someapp").program(ProgramType.SERVICE, "s");
    RunRecordDetail.Builder runRecordBuilder = RunRecordDetail.builder();
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
  private void setUpProgramHeartBeats(RunRecordDetail runRecordMeta,
                                      long startTime, long endTime, long interval) {
    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      for (long time = startTime + interval; time < endTime; time += interval) {
        programHeartbeatTable.writeRunRecordMeta(runRecordMeta, time);
      }
    });
  }

  @Test
  public void testScanningProgramStatus() throws Exception {
    // p1 : x           ->    x + 5
    // scanning from x -> x + 4 -> return one record with status running
    // scanning from x -> x + 5min + 1s -> return one record with status killed

    //  write program status "running" for program 1 starting at x
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId = RunIds.generate();
    RunRecordDetail.Builder metaRunningBuilder = getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaRunningBuilder.setStatus(ProgramRunStatus.RUNNING);
    metaRunningBuilder.setRunTime(startTime1);
    RunRecordDetail metaRunning = metaRunningBuilder.build();
    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      programHeartbeatTable.writeRunRecordMeta(metaRunning, startTime1);
    });

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long heartBeatEndTime = startTime1 + TimeUnit.MINUTES.toSeconds(4);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(metaRunning, startTime1, heartBeatEndTime, interval);

    long programEndTime = startTime1 + TimeUnit.MINUTES.toSeconds(5);

    RunRecordDetail.Builder metaKilledBuilder = getMockRunRecordMeta(NamespaceId.DEFAULT, runId);
    metaKilledBuilder.setStatus(ProgramRunStatus.KILLED);
    metaKilledBuilder.setStopTime(programEndTime);
    RunRecordDetail metaKilled = metaKilledBuilder.build();
    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      programHeartbeatTable.writeRunRecordMeta(metaKilled, programEndTime);
    });

    // perform scan checks
    TransactionRunners.run(transactionRunner, context -> {
      ProgramHeartbeatTable programHeartbeatTable = new ProgramHeartbeatTable(context);
      // end row key is exclusive, scanning from x : x + 5 should only return run1
      Collection<RunRecordDetail> runRecordMetaList =
        programHeartbeatTable.scan(startTime1, heartBeatEndTime, ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(1 , runRecordMetaList.size());
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMetaList.iterator().next().getStatus());
      Assert.assertEquals(metaRunning, runRecordMetaList.iterator().next());
      // scanning from x : x + (5 min + 1s), should return run1 with status KILLED
      runRecordMetaList = programHeartbeatTable.scan(startTime1, programEndTime + 1,
                                                     ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      Assert.assertEquals(1 , runRecordMetaList.size());
      Assert.assertEquals(metaKilled, runRecordMetaList.iterator().next());
    });
  }
}
