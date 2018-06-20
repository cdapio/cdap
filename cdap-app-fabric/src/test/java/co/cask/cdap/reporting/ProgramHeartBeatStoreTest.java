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
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProgramHeartBeatStoreTest {
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
    ProgramHeartbeatStore programHeartbeatStore = getHeartBeatStore("testBeats");
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, ARTIFACT_ID.toString());
    systemArguments.put(ProgramOptionConstants.PRINCIPAL, "userA");
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());

    ProgramRunId programRunId1 = programId.run(RunIds.generate());

    Map<String, String> properties = new HashMap<>();

    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId1));
    properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(System.currentTimeMillis()));
    properties.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.RUNNING.name());
    properties.put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions));

    long startTime1 = System.currentTimeMillis();
    long endTime = startTime1 + TimeUnit.MINUTES.toMillis(10);
    TransactionExecutor txnl = getTxExecutor(programHeartbeatStore);


    // write heart beat messages for 10 minutes for this program run.
    for (long time = startTime1; time <= endTime; time += TimeUnit.MINUTES.toMillis(1)) {
      properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(time));
      Notification notification = new Notification(Notification.Type.HEART_BEAT, properties);
      txnl.execute(() -> {
        programHeartbeatStore.writeProgramHeartBeatStatus(notification);
      });
    }
    ProgramRunId programRunId2 = programId.run(RunIds.generate());
    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId2));
    long startTime2 = endTime - TimeUnit.MINUTES.toMillis(5);
    // write heart beat messages for 10 minutes for this program run.
    for (long time = startTime2; time <= endTime; time += TimeUnit.MINUTES.toMillis(1)) {
      properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(time));
      Notification notification = new Notification(Notification.Type.HEART_BEAT, properties);
      txnl.execute(() -> {
        programHeartbeatStore.writeProgramHeartBeatStatus(notification);
      });
    }

    // we have program run1 -> start : time1 and end : time1 + 10 min
    // we have program run2 -> start : time1 + 5 min and end : time1 + 10 min
    // lets perform scan to verify results

    txnl.execute(() -> {
      Assert.assertEquals(1 , programHeartbeatStore.scan(startTime1, startTime2).size());
      Assert.assertEquals(2 , programHeartbeatStore.scan(startTime1, endTime).size());
      Assert.assertEquals(2 , programHeartbeatStore.scan(startTime2, endTime).size());
    });
  }

  private TransactionExecutor getTxExecutor(ProgramHeartbeatStore programHeartbeatStore) {
    return txExecutorFactory.createExecutor(Collections.singleton(programHeartbeatStore));
  }

  private ProgramHeartbeatStore getHeartBeatStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.SYSTEM.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);
    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.of(), null);
    Assert.assertNotNull(table);
    return new ProgramHeartbeatStore(table, cConf);
  }
}
