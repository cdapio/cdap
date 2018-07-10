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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.reporting.ProgramHeartbeatDataset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests program run state persistence.
 */
public class ProgramNotificationSubscriberServiceTest {
  private static final String SYSTEM_METRIC_PREFIX = "system.";

  private static Injector injector;
  private static Table appMetaTable;
  private static AppMetadataStore metadataStoreDataset;
  private static ProgramHeartbeatDataset programHeartbeatDataset;
  private static TransactionExecutor txnl;
  private static TransactionExecutor heartBeatTxnl;
  private static ProgramStateWriter programStateWriter;

  @BeforeClass
  public static void setupClass() throws Exception {
    injector = AppFabricTestHelper.getInjector();
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    // we only want to process and check program status messages processed by heart beat store, so set high value
    cConf.set(Constants.ProgramHeartbeat.HEARTBEAT_INTERVAL_SECONDS, String.valueOf(TimeUnit.HOURS.toSeconds(1)));
    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    TransactionExecutorFactory txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);

    DatasetId storeTable = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
    appMetaTable = DatasetsUtil.getOrCreateDataset(datasetFramework, storeTable, Table.class.getName(),
                                                   DatasetProperties.EMPTY, Collections.emptyMap());
    metadataStoreDataset = new AppMetadataStore(appMetaTable, cConf);

    DatasetId heartbeatDataset = NamespaceId.SYSTEM.dataset(Constants.ProgramHeartbeat.TABLE);
    Table heartbeatTable = DatasetsUtil.getOrCreateDataset(datasetFramework, heartbeatDataset, Table.class.getName(),
                                                     DatasetProperties.EMPTY, Collections.emptyMap());

    txnl = txExecutorFactory.createExecutor(Collections.singleton(metadataStoreDataset));
    programHeartbeatDataset = new ProgramHeartbeatDataset(heartbeatTable);
    heartBeatTxnl = txExecutorFactory.createExecutor(Collections.singleton(programHeartbeatDataset));
    programStateWriter = injector.getInstance(ProgramStateWriter.class);
  }

  @After
  public void cleanupTest() throws Exception {
    txnl.execute(() -> {
      Scanner scanner = appMetaTable.scan(null, null);
      Row row;
      while ((row = scanner.next()) != null) {
        appMetaTable.delete(row.getRow());
      }
    });
  }

  @Test
  public void testAppSpecNotRequiredToWriteState() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    programStateWriter.start(runId, programOptions, null, programDescriptor);

    Tasks.waitFor(ProgramRunStatus.STARTING, () -> txnl.execute(() -> {
                    RunRecordMeta meta = metadataStoreDataset.getRun(runId);
                    if (meta == null) {
                      return null;
                    }
                    Assert.assertEquals(artifactId, meta.getArtifactId());
                    return meta.getStatus();
                  }),
                  10, TimeUnit.SECONDS);
    programStateWriter.completed(runId);
  }

  @Test
  public void testMetricsEmit() throws Exception {
    ProfileService profileService = injector.getInstance(ProfileService.class);
    // create my profile
    ProfileId myProfile = NamespaceId.DEFAULT.profile("MyProfile");
    profileService.saveProfile(myProfile, Profile.NATIVE);

    ProgramId programId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    RunId runId = RunIds.generate(System.currentTimeMillis());
    ProgramRunId programRunId = programId.run(runId.getId());
    Map<String, String> systemArgs = Collections.singletonMap(SystemArguments.PROFILE_NAME, myProfile.getScopedName());

    long startTime = System.currentTimeMillis();
    // record the program status in app meta store
    txnl.execute(() -> {
      int sourceId = 0;
      metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), systemArgs,
                                                     AppFabricTestHelper.createSourceId(++sourceId), artifactId);
      // using 3 nodes
      metadataStoreDataset.recordProgramProvisioned(programRunId, 3, AppFabricTestHelper.createSourceId(++sourceId));
      metadataStoreDataset.recordProgramStart(programRunId, null, systemArgs,
                                              AppFabricTestHelper.createSourceId(++sourceId));
      metadataStoreDataset.recordProgramRunning(programRunId, startTime + 60000, null,
                                                AppFabricTestHelper.createSourceId(++sourceId));
    });

    programStateWriter.completed(programRunId);
    MetricStore metricStore = injector.getInstance(MetricStore.class);
    Tasks.waitFor(1L, () -> getMetric(metricStore, programRunId, myProfile,
                                      SYSTEM_METRIC_PREFIX + Constants.Metrics.Program.PROGRAM_COMPLETED_RUNS),
                  10, TimeUnit.SECONDS);

    // verify the metrics
    Assert.assertEquals(1L, getMetric(metricStore, programRunId, myProfile,
                                      SYSTEM_METRIC_PREFIX + Constants.Metrics.Program.PROGRAM_COMPLETED_RUNS));
    Assert.assertEquals(0L, getMetric(metricStore, programRunId, myProfile,
                                      SYSTEM_METRIC_PREFIX + Constants.Metrics.Program.PROGRAM_KILLED_RUNS));
    Assert.assertEquals(0L, getMetric(metricStore, programRunId, myProfile,
                                      SYSTEM_METRIC_PREFIX + Constants.Metrics.Program.PROGRAM_FAILED_RUNS));
    metricStore.deleteAll();
  }

  @Test
  public void testHeartBeatStoreForProgramStatusMessages() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("someapp", "1.0-SNAPSHOT").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);
    heartBeatTxnl.execute(() -> {
      programStateWriter.start(runId, programOptions, null, programDescriptor);
    });
    checkProgramStatus(artifactId, runId, ProgramRunStatus.STARTING);
    long startTime = System.currentTimeMillis();
    heartBeatTxnl.execute(() -> {
      programStateWriter.running(runId, null);
    });

    // perform scan on heart beat store - ensure latest message notification is running
    checkProgramStatus(artifactId, runId, ProgramRunStatus.RUNNING);
    heartbeatDatasetStatusCheck(startTime, ProgramRunStatus.RUNNING);

    long suspendTime = System.currentTimeMillis();
    heartBeatTxnl.execute(() -> {
      programStateWriter.suspend(runId);
    });
    // perform scan on heart beat store - ensure latest message notification is suspended
    checkProgramStatus(artifactId, runId, ProgramRunStatus.SUSPENDED);
    heartbeatDatasetStatusCheck(suspendTime, ProgramRunStatus.SUSPENDED);

    long resumeTime = System.currentTimeMillis();
    heartBeatTxnl.execute(() -> {
      programStateWriter.resume(runId);
    });
    // app metadata records as RUNNING
    checkProgramStatus(artifactId, runId, ProgramRunStatus.RUNNING);
    // heart beat messages wont have been sent due to high interval. resuming program will be recorded as running
    // in run record by app meta
    heartbeatDatasetStatusCheck(resumeTime, ProgramRunStatus.RUNNING);
    // killed status check after error
    long stopTime = System.currentTimeMillis();
    heartBeatTxnl.execute(() -> {
      programStateWriter.error(runId, new Throwable("Testing"));
    });
    checkProgramStatus(artifactId, runId, ProgramRunStatus.FAILED);
    heartbeatDatasetStatusCheck(stopTime, ProgramRunStatus.FAILED);
  }

  private void checkProgramStatus(ArtifactId artifactId, ProgramRunId runId, ProgramRunStatus expectedStatus)
    throws InterruptedException, ExecutionException, TimeoutException {
    Tasks.waitFor(expectedStatus, () -> txnl.execute(() -> {
                    RunRecordMeta meta = metadataStoreDataset.getRun(runId);
                    if (meta == null) {
                      return null;
                    }
                    Assert.assertEquals(artifactId, meta.getArtifactId());
                    return meta.getStatus();
                  }),
                  10, TimeUnit.SECONDS);
  }

  private void heartbeatDatasetStatusCheck(long startTime, ProgramRunStatus expectedStatus)
    throws InterruptedException, ExecutionException, TimeoutException {
    Tasks.waitFor(expectedStatus, () -> heartBeatTxnl.execute(() -> {
      Collection<RunRecordMeta> runRecordMetas =
        // programHeartbeatDataset uses seconds for timeunit for recording runrecords
        programHeartbeatDataset.scan(
          TimeUnit.MILLISECONDS.toSeconds(startTime), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
          ImmutableSet.of(NamespaceId.DEFAULT.getNamespace()));
      if (runRecordMetas.size() == 0) {
        return null;
      }
      Assert.assertEquals(1, runRecordMetas.size());
      return runRecordMetas.iterator().next().getStatus();
    }), 10, TimeUnit.SECONDS);
  }

  private long getMetric(MetricStore metricStore, ProgramRunId programRunId, ProfileId profileId, String metricName) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getScopedName())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .build();
    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<>());
    Collection<MetricTimeSeries> result = metricStore.query(query);
    if (result.isEmpty()) {
      return 0;
    }
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }
    return timeValues.get(0).getValue();
  }
}
