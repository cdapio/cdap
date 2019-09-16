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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordMeta;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.reporting.ProgramHeartbeatTable;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
  private static CConfiguration cConf;
  private static ProgramStateWriter programStateWriter;
  private static WorkflowStateWriter workflowStateWriter;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setupClass() {
    injector = AppFabricTestHelper.getInjector();
    cConf = injector.getInstance(CConfiguration.class);
    // we only want to process and check program status messages processed by heart beat store, so set high value
    cConf.set(Constants.ProgramHeartbeat.HEARTBEAT_INTERVAL_SECONDS, String.valueOf(TimeUnit.HOURS.toSeconds(1)));

    programStateWriter = injector.getInstance(ProgramStateWriter.class);
    workflowStateWriter = injector.getInstance(WorkflowStateWriter.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore.create(context).deleteAllAppMetadataTables();
    });
  }

  @Test
  public void testWorkflowInnerPrograms() throws Exception {
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, ProgramStateWorkflowApp.class, null, cConf);

    ProgramRunId workflowRunId = NamespaceId.DEFAULT
      .app(ProgramStateWorkflowApp.class.getSimpleName())
      .workflow(ProgramStateWorkflowApp.ProgramStateWorkflow.class.getSimpleName())
      .run(RunIds.generate());

    ApplicationSpecification appSpec = TransactionRunners.run(transactionRunner, context -> {
      return AppMetadataStore.create(context).getApplication(workflowRunId.getParent().getParent()).getSpec();
    });

    ProgramDescriptor programDescriptor = new ProgramDescriptor(workflowRunId.getParent(), appSpec);

    // Start and run the workflow
    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    systemArgs.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());

    programStateWriter.start(workflowRunId, new SimpleProgramOptions(workflowRunId.getParent(),
                                                                     new BasicArguments(systemArgs),
                                                                     new BasicArguments()), null, programDescriptor);
    programStateWriter.running(workflowRunId, null);

    ProgramRunId mrRunId = workflowRunId.getParent().getParent()
      .mr(ProgramStateWorkflowApp.ProgramStateMR.class.getSimpleName()).run(RunIds.generate());
    ProgramRunId sparkRunId = workflowRunId.getParent().getParent()
      .spark(ProgramStateWorkflowApp.ProgramStateSpark.class.getSimpleName()).run(RunIds.generate());
    ProgramId sparkId2 = workflowRunId.getParent().getParent()
      .spark(ProgramStateWorkflowApp.ProgramStateSpark2.class.getSimpleName());

    // Start and run the MR and Spark inside
    for (ProgramRunId programRunId : Arrays.asList(mrRunId, sparkRunId)) {
      workflowStateWriter.addWorkflowNodeState(workflowRunId, new WorkflowNodeStateDetail(programRunId.getProgram(),
                                                                                          NodeStatus.STARTING));
      workflowStateWriter.addWorkflowNodeState(workflowRunId, new WorkflowNodeStateDetail(programRunId.getProgram(),
                                                                                          NodeStatus.RUNNING));

      systemArgs = new HashMap<>(systemArgs);
      systemArgs.put(ProgramOptionConstants.RUN_ID, programRunId.getRun());
      systemArgs.put(ProgramOptionConstants.WORKFLOW_NAME, workflowRunId.getProgram());
      systemArgs.put(ProgramOptionConstants.WORKFLOW_RUN_ID, workflowRunId.getRun());
      systemArgs.put(ProgramOptionConstants.WORKFLOW_NODE_ID, programRunId.getProgram());
      systemArgs.put(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW, programRunId.getProgram());
      programStateWriter.start(programRunId,
                               new SimpleProgramOptions(programRunId.getParent(), new BasicArguments(systemArgs),
                                                        new BasicArguments()), null, programDescriptor);
      programStateWriter.running(programRunId, null);

      // Wait for the inner program running
      Tasks.waitFor(ProgramRunStatus.RUNNING, () -> TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
        RunRecordMeta meta = metadataStoreDataset.getRun(programRunId);
        if (meta == null) {
          return null;
        }
        return meta.getStatus();
      }), 10, TimeUnit.SECONDS);
    }

    // Stop the Spark normally
    programStateWriter.completed(sparkRunId);

    // Error out the Workflow without stopping the MR
    programStateWriter.error(workflowRunId, new IllegalStateException("Explicitly error out"));

    // Wait for the Workflow state changed to failed
    Tasks.waitFor(ProgramRunStatus.FAILED, () -> TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      RunRecordMeta meta = metadataStoreDataset.getRun(workflowRunId);
      if (meta == null) {
        return null;
      }
      return meta.getStatus();
    }), 10000, TimeUnit.SECONDS);

    // The MR run record should be changed to ERROR state as well (without race)
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      RunRecordMeta meta = metadataStoreDataset.getRun(mrRunId);
      Assert.assertNotNull(meta);
      Assert.assertEquals(ProgramRunStatus.FAILED, meta.getStatus());
    });

    // The Spark run record should stay as COMPLETED
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      RunRecordMeta meta = metadataStoreDataset.getRun(sparkRunId);
      Assert.assertNotNull(meta);
      Assert.assertEquals(ProgramRunStatus.COMPLETED, meta.getStatus());
    });

    // Since the Spark2 program hasn't been executed, there should be no run record
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      Map<ProgramRunId, RunRecordMeta> runs = metadataStoreDataset.getRuns(sparkId2, ProgramRunStatus.ALL,
                                                                           0, Long.MAX_VALUE, 100, null);
      Assert.assertTrue(runs.isEmpty());
    });
  }

  @Test
  public void testAppSpecNotRequiredToWriteState() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    systemArguments.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    programStateWriter.start(runId, programOptions, null, programDescriptor);

    Tasks.waitFor(ProgramRunStatus.STARTING, () -> TransactionRunners.run(transactionRunner, context -> {
                    AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
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
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
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
    systemArguments.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());
    ProgramRunId runId = programId.run(RunIds.generate());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.start(runId, programOptions, null, programDescriptor);
    });
    checkProgramStatus(artifactId, runId, ProgramRunStatus.STARTING);
    long startTime = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.running(runId, null);
    });

    // perform scan on heart beat store - ensure latest message notification is running
    checkProgramStatus(artifactId, runId, ProgramRunStatus.RUNNING);
    heartbeatDatasetStatusCheck(startTime, ProgramRunStatus.RUNNING);

    long suspendTime = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.suspend(runId);
    });
    // perform scan on heart beat store - ensure latest message notification is suspended
    checkProgramStatus(artifactId, runId, ProgramRunStatus.SUSPENDED);
    heartbeatDatasetStatusCheck(suspendTime, ProgramRunStatus.SUSPENDED);

    long resumeTime = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.resume(runId);
    });
    // app metadata records as RUNNING
    checkProgramStatus(artifactId, runId, ProgramRunStatus.RUNNING);
    // heart beat messages wont have been sent due to high interval. resuming program will be recorded as running
    // in run record by app meta
    heartbeatDatasetStatusCheck(resumeTime, ProgramRunStatus.RUNNING);
    // killed status check after error
    long stopTime = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.error(runId, new Throwable("Testing"));
    });
    checkProgramStatus(artifactId, runId, ProgramRunStatus.FAILED);
    heartbeatDatasetStatusCheck(stopTime, ProgramRunStatus.FAILED);

    ProgramRunId runId2 = programId.run(RunIds.generate());
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.start(runId2, programOptions, null, programDescriptor);
    });
    checkProgramStatus(artifactId, runId2, ProgramRunStatus.STARTING);
    startTime = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.running(runId2, null);
    });

    // perform scan on heart beat store - ensure latest message notification is running
    checkProgramStatus(artifactId, runId2, ProgramRunStatus.RUNNING);
    heartbeatDatasetStatusCheck(startTime, ProgramRunStatus.RUNNING);
    // completed status check
    stopTime = System.currentTimeMillis();

    TransactionRunners.run(transactionRunner, context -> {
      programStateWriter.completed(runId2);
    });
    checkProgramStatus(artifactId, runId2, ProgramRunStatus.COMPLETED);
    heartbeatDatasetStatusCheck(stopTime, ProgramRunStatus.COMPLETED);
  }

  private void checkProgramStatus(ArtifactId artifactId, ProgramRunId runId, ProgramRunStatus expectedStatus)
    throws InterruptedException, ExecutionException, TimeoutException {
    Tasks.waitFor(expectedStatus, () -> TransactionRunners.run(transactionRunner, context -> {
                    AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
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
    Tasks.waitFor(expectedStatus, () -> TransactionRunners.run(transactionRunner, context -> {
      Collection<RunRecordMeta> runRecordMetas =
        // programHeartbeatTable uses seconds for timeunit for recording runrecords
        new ProgramHeartbeatTable(context).scan(
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
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
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
