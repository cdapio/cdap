/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.lang.FunctionWithException;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test AppMetadataStore.
 */
public abstract class AppMetadataStoreTest {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStoreTest.class);

  protected static TransactionRunner transactionRunner;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final Map<String, String> SINGLETON_PROFILE_MAP =
    Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());

  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong();
  private final Long creationTimeMillis = System.currentTimeMillis();

  @Before
  public void before() {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.deleteAllAppMetadataTables();
    });
  }

  private void recordProvisionAndStart(ProgramRunId programRunId, AppMetadataStore metadataStoreDataset)
    throws IOException {
    metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                   ARTIFACT_ID);
    metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
    metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
  }

  @Test
  public void testSmallerSourceIdRecords() throws Exception {
    // STARTING status is persisted with the largest sourceId
    assertPersistedStatus(100L, 10L, 1L, ProgramRunStatus.STARTING);
    assertPersistedStatus(100L, 1L, 10L, ProgramRunStatus.STARTING);
    // RUNNING status is persisted with the largest sourceId
    assertPersistedStatus(1L, 100L, 10L, ProgramRunStatus.RUNNING);
    // KILLED status is persisted with the largest sourceId
    assertPersistedStatus(1L, 10L, 100L, ProgramRunStatus.KILLED);
  }

  @Test
  public void testPendingToCompletedIsIgnored() throws Exception {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    RunId runId1 = RunIds.generate();
    ProgramRunId programRunId = program.run(runId1);

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                                     AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                     ARTIFACT_ID);
      metadataStoreDataset.recordProgramStop(programRunId, 0, ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.PENDING, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testStoppingStatusPersistence() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId, programRunId, metadataStoreDataset);
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.STOPPING, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testStoppingToCompletedStatePersistence() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId, programRunId, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId, 0, ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testStoppingToKilledStatePersistence() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId, programRunId, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId, 0, ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.KILLED, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testStoppingToFailedStatePersistence() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId, programRunId, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId, 0, ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testStoppingToOtherActiveStatesAreIgnored() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId1 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId1 = program.run(runId1);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId1, programRunId1, metadataStoreDataset);
      metadataStoreDataset.recordProgramProvisioning(programRunId1, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                                     AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                     ARTIFACT_ID);
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      Assert.assertEquals(ProgramRunStatus.STOPPING, runRecordMeta.getStatus());
    });

    final RunId runId2 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId2 = program.run(runId1);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId2, programRunId2, metadataStoreDataset);
      metadataStoreDataset.recordProgramProvisioned(programRunId2, 0,
                                                    AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId2);
      Assert.assertEquals(ProgramRunStatus.STOPPING, runRecordMeta.getStatus());
    });

    final RunId runId3 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId3 = program.run(runId1);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProgramStopping(runId3, programRunId3, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunning(programRunId3, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId3);
      Assert.assertEquals(ProgramRunStatus.STOPPING, runRecordMeta.getStatus());
    });
  }

  private void recordProgramStopping(RunId runId1, ProgramRunId programRunId1, AppMetadataStore metadataStoreDataset)
    throws IOException {
    recordProvisionAndStart(programRunId1, metadataStoreDataset);
    metadataStoreDataset.recordProgramRunning(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                              null,
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
    metadataStoreDataset.recordProgramStopping(programRunId1,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                               RunIds.getTime(runId1, TimeUnit.SECONDS),
                                               RunIds.getTime(runId1, TimeUnit.SECONDS) + 1000);
  }

  @Test
  public void testValidStoppingStatusPersistence() {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId1 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId1 = program.run(runId1);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProvisionAndStart(programRunId1, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunning(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStopping(programRunId1,
                                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                 RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                 0);
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      Assert.assertEquals(ProgramRunStatus.STOPPING, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testInvalidStatusPersistence() throws Exception {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId1 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId1 = program.run(runId1);
    final AtomicLong sourceId = new AtomicLong();
    // No status can be persisted if STARTING is not present
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramRunning(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId2 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId2 = program.run(runId2);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramSuspend(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      metadataStoreDataset.recordProgramResumed(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId2);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId3 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId3 = program.run(runId3);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId3);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId4 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId4 = program.run(runId4);
    // Once a stop status is reached, any incoming status will be ignored
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProvisionAndStart(programRunId4, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId4);
      // KILLED after COMPLETED is ignored
      Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecordMeta.getStatus());
    });
    final RunId runId5 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId5 = program.run(runId5);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProvisionAndStart(programRunId5, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId5);
      // COMPLETED after FAILED is ignored
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
    });
    final RunId runId6 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId6 = program.run(runId6);
    Long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // STARTING status will be ignored if there's any existing record
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      recordProvisionAndStart(programRunId6, metadataStoreDataset);
      // CDAP-13551 - seems like the program should not be allowed to suspend when in starting state
      metadataStoreDataset.recordProgramSuspend(programRunId6,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                currentTime);
      metadataStoreDataset.recordProgramStart(programRunId6, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId6);
      // STARTING status is ignored since there's an existing SUSPENDED record
      Assert.assertEquals(ProgramRunStatus.SUSPENDED, runRecordMeta.getStatus());
      Assert.assertEquals(currentTime, runRecordMeta.getSuspendTs());
    });
    final RunId runId7 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId7 = program.run(runId7);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      long startTime = RunIds.getTime(runId7, TimeUnit.SECONDS);
      recordProvisionAndStart(programRunId7, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunning(programRunId7, startTime, null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStart(programRunId7, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordDetail runRecordMeta = metadataStoreDataset.getRun(programRunId7);
      // STARTING status is ignored since there's an existing RUNNING record
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMeta.getStatus());
    });
  }

  private void assertPersistedStatus(final long startSourceId, final long runningSourceId,
                                     final long killedSourceId, final ProgramRunStatus expectedRunStatus)
    throws Exception {
    // Add some run records
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final AtomicReference<RunRecordDetail> resultRecord = new AtomicReference<>();
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramProvisioning(programRunId, null, SINGLETON_PROFILE_MAP,
                                                     AppFabricTestHelper.createSourceId(startSourceId), ARTIFACT_ID);
      metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                    AppFabricTestHelper.createSourceId(startSourceId + 1));
      metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                              AppFabricTestHelper.createSourceId(startSourceId + 2));
      metadataStoreDataset.recordProgramRunning(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                                null, AppFabricTestHelper.createSourceId(runningSourceId));
      metadataStoreDataset.recordProgramStop(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED,
                                             null, AppFabricTestHelper.createSourceId(killedSourceId));
      resultRecord.set(metadataStoreDataset.getRun(programRunId));
    });
    Assert.assertEquals(expectedRunStatus, resultRecord.get().getStatus());
  }

  @Test
  public void testScanRunningInRangeWithBatch() throws Exception {
    // Add some run records
    TreeSet<Long> expected = new TreeSet<>();
    for (int i = 0; i < 100; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app" + i);
      final ProgramId program = application.program(ProgramType.values()[i % ProgramType.values().length],
                                                    "program" + i);
      final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
      final ProgramRunId programRunId = program.run(runId);
      expected.add(RunIds.getTime(runId, TimeUnit.MILLISECONDS));
      // Start the program and stop it
      final int j = i;
      // A sourceId to keep incrementing for each call of app meta data store persisting
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
        recordProvisionAndStart(programRunId, metadataStoreDataset);
        metadataStoreDataset.recordProgramRunning(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS) + 1, null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        metadataStoreDataset.recordProgramStop(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
          STOP_STATUSES.get(j % STOP_STATUSES.size()),
          null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    // Run full scan
    runScan(expected, 0, Long.MAX_VALUE);

    // In all below assertions, TreeSet and metadataStore both have start time inclusive and end time exclusive.
    // Run the scan with time limit
    runScan(expected.subSet(30 * 10000L, 90 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(90 * 10000));

    runScan(expected.subSet(90 * 10000L, 101 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(90 * 10000), TimeUnit.MILLISECONDS.toSeconds(101 * 10000));

    // After range
    runScan(expected.subSet(101 * 10000L, 200 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(101 * 10000), TimeUnit.MILLISECONDS.toSeconds(200 * 10000));

    // Identical start and end time
    runScan(expected.subSet(31 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(31 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // One unit difference between start and end time
    runScan(expected.subSet(30 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // Before range
    runScan(expected.subSet(1000L, 10000L),
            TimeUnit.MILLISECONDS.toSeconds(1000), TimeUnit.MILLISECONDS.toSeconds(10000));
  }

  private void runScan(final Set<Long> expected, final long startTime, final long stopTime) {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      // Run the scan
      Set<Long> actual = metadataStoreDataset.getRunningInRangeForStatus("runRecordCompleted", startTime, stopTime)
        .stream()
        .map(id -> RunIds.getTime(id, TimeUnit.MILLISECONDS))
        .collect(Collectors.toCollection(TreeSet::new));

      Assert.assertEquals(expected, actual);
    });
  }

  @Test
  public void testGetRuns() throws Exception {
    // Add some run records
    final Set<String> expected = new TreeSet<>();
    final Set<String> expectedHalf = new TreeSet<>();
    final Set<ProgramRunId> programRunIdSet = new HashSet<>();
    final Set<ProgramRunId> programRunIdSetHalf = new HashSet<>();
    for (int i = 0; i < 100; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app");
      final ProgramId program = application.program(ProgramType.SERVICE, "program");
      final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
      expected.add(runId.toString());
      final int index = i;

      // Add every other runId
      if ((i % 2) == 0) {
        expectedHalf.add(runId.toString());
      }

      ProgramRunId programRunId = program.run(runId);
      programRunIdSet.add(programRunId);

      //Add every other programRunId
      if ((i % 2) == 0) {
        programRunIdSetHalf.add(programRunId);
      }

      // A sourceId to keep incrementing for each call of app meta data store persisting
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
        // Start the program and stop it
        recordProvisionAndStart(programRunId, metadataStoreDataset);
        metadataStoreDataset.recordProgramRunning(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS), null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        metadataStoreDataset.recordProgramStop(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.values()[index % ProgramRunStatus.values().length],
                                               null,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      Map<ProgramRunId, RunRecordDetail> runMap = metadataStoreDataset.getRuns(programRunIdSet);
      Set<String> actual = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> entry : runMap.entrySet()) {
        actual.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expected, actual);


      Map<ProgramRunId, RunRecordDetail> runMapHalf = metadataStoreDataset.getRuns(programRunIdSetHalf);
      Set<String> actualHalf = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> entry : runMapHalf.entrySet()) {
        actualHalf.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expectedHalf, actualHalf);
    });
  }

  @Test
  public void testGetActiveRuns() throws Exception {
    // write a run record for each state for two programs in two apps in two namespaces
    String app1 = "app1";
    String app2 = "app2";
    String program1 = "prog1";
    String program2 = "prog2";

    Collection<NamespaceId> namespaces = Arrays.asList(new NamespaceId("ns1"), new NamespaceId("ns2"));
    Collection<ApplicationId> apps = namespaces.stream()
      .flatMap(ns -> Stream.of(ns.app(app1), ns.app(app2)))
      .collect(Collectors.toList());
    Collection<ProgramId> programs = apps.stream()
      .flatMap(app -> Stream.of(app.mr(program1), app.mr(program2)))
      .collect(Collectors.toList());

    for (ProgramId programId : programs) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        // one run in pending state
        ProgramRunId runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);

        // one run in starting state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStart(runId, UUID.randomUUID().toString(), Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one run in running state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one in suspended state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramSuspend(runId, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                   System.currentTimeMillis());

        // one run in stopping state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStopping(runId, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                    System.currentTimeMillis(), System.currentTimeMillis() + 1000);

        // one run in each stopped state
        for (ProgramRunStatus runStatus : ProgramRunStatus.values()) {
          if (!runStatus.isEndState()) {
            continue;
          }
          runId = programId.run(RunIds.generate());
          store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
          store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          twillRunId = UUID.randomUUID().toString();
          store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          store.recordProgramStop(runId, System.currentTimeMillis(), runStatus, null,
                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        }
      });
    }

    Set<ProgramRunStatus> activeStates = new HashSet<>();
    activeStates.add(ProgramRunStatus.PENDING);
    activeStates.add(ProgramRunStatus.STARTING);
    activeStates.add(ProgramRunStatus.RUNNING);
    activeStates.add(ProgramRunStatus.SUSPENDED);
    activeStates.add(ProgramRunStatus.STOPPING);

    // test the instance level method and namespace level method
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Map<ProgramId, Set<ProgramRunStatus>> allExpected = new HashMap<>();
      Map<ProgramId, Set<ProgramRunStatus>> allActual = new HashMap<>();
      // check active runs per namespace
      for (NamespaceId namespace : namespaces) {
        Map<ProgramRunId, RunRecordDetail> activeRuns = store.getActiveRuns(namespace);

        // we expect 4 runs per program, with 4 programs in each namespace
        Map<ProgramId, Set<ProgramRunStatus>> expected = new HashMap<>();
        expected.put(namespace.app(app1).mr(program1), activeStates);
        expected.put(namespace.app(app1).mr(program2), activeStates);
        expected.put(namespace.app(app2).mr(program1), activeStates);
        expected.put(namespace.app(app2).mr(program2), activeStates);

        Map<ProgramId, Set<ProgramRunStatus>> actual = new HashMap<>();
        actual.put(namespace.app(app1).mr(program1), new HashSet<>());
        actual.put(namespace.app(app1).mr(program2), new HashSet<>());
        actual.put(namespace.app(app2).mr(program1), new HashSet<>());
        actual.put(namespace.app(app2).mr(program2), new HashSet<>());
        allActual.putAll(actual);
        for (Map.Entry<ProgramRunId, RunRecordDetail> activeRun : activeRuns.entrySet()) {
          ProgramId programId = activeRun.getKey().getParent();
          Assert.assertTrue("Unexpected program returned: " + programId,
                            actual.containsKey(activeRun.getKey().getParent()));
          actual.get(programId).add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(expected, actual);
        allExpected.putAll(expected);
      }

      // test the instance level method
      for (Map.Entry<ProgramRunId, RunRecordDetail> activeRun : store.getActiveRuns(x -> true).entrySet()) {
        ProgramId programId = activeRun.getKey().getParent();
        Assert.assertTrue("Unexpected program returned: " + programId,
                          allActual.containsKey(activeRun.getKey().getParent()));
        allActual.get(programId).add(activeRun.getValue().getStatus());
      }
      Assert.assertEquals(allExpected, allActual);

      // test the count-all method
      Assert.assertEquals(store.getActiveRuns(x -> true).size(), store.countActiveRuns(null));
      Assert.assertEquals(store.getActiveRuns(x -> true).size(), store.countActiveRuns(100));
      Assert.assertEquals(2, store.countActiveRuns(2));
    });

    // check active runs per app
    for (ApplicationId app : apps) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        Map<ProgramRunId, RunRecordDetail> activeRuns = store.getActiveRuns(app);

        // we expect 3 runs per program, with 2 programs in each app
        Map<ProgramId, Set<ProgramRunStatus>> expected = new HashMap<>();
        expected.put(app.mr(program1), activeStates);
        expected.put(app.mr(program2), activeStates);

        Map<ProgramId, Set<ProgramRunStatus>> actual = new HashMap<>();
        actual.put(app.mr(program1), new HashSet<>());
        actual.put(app.mr(program2), new HashSet<>());
        for (Map.Entry<ProgramRunId, RunRecordDetail> activeRun : activeRuns.entrySet()) {
          ProgramId programId = activeRun.getKey().getParent();
          Assert.assertTrue("Unexpected program returned: " + programId,
                            actual.containsKey(activeRun.getKey().getParent()));
          actual.get(programId).add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(expected, actual);
      });
    }

    // check active runs per program
    for (ProgramId program : programs) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        Map<ProgramRunId, RunRecordDetail> activeRuns = store.getActiveRuns(program);

        Set<ProgramRunStatus> actual = new HashSet<>();
        for (Map.Entry<ProgramRunId, RunRecordDetail> activeRun : activeRuns.entrySet()) {
          Assert.assertEquals(program, activeRun.getKey().getParent());
          actual.add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(activeStates, actual);
      });
    }
  }

  @Test
  public void testGetNumberOfActiveRunsAcrossVersions() throws Exception {
    // write a run record for each state for 2 programs in 1 app (with 2 versions) in 2 namespaces
    String app1 = "app1";
    String program1 = "prog1";
    String program2 = "prog2";
    String v1 = "test-version-1";
    String v2 = "test-version-2";

    Collection<NamespaceId> namespaces = Arrays.asList(new NamespaceId("ns1"), new NamespaceId("ns2"));
    Collection<ApplicationId> apps = namespaces.stream()
        .flatMap(ns -> Stream.of(ns.app(app1, v1), ns.app(app1, v2)))
        .collect(Collectors.toList());
    Collection<ProgramId> programs = apps.stream()
        .flatMap(app -> Stream.of(app.mr(program1), app.mr(program2)))
        .collect(Collectors.toList());

    // collect the program references to a set, the size of this set should be 4 (2 namespaces * 2 programs)
    Collection<ProgramReference> programRefs = programs.stream()
        .map(ProgramId::getProgramReference)
        .collect(Collectors.toSet());
    Assert.assertEquals(4, programRefs.size());

    for (ProgramId programId : programs) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        // one in REJECTED state
        ProgramRunId runId = programId.run(RunIds.generate());
        store.recordProgramRejected(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);

        // one run in pending state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);

        // one run in starting state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStart(runId, UUID.randomUUID().toString(), Collections.emptyMap(),
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one run in running state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one in suspended state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramSuspend(runId, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
            System.currentTimeMillis());

        // one run in stopping state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStopping(runId, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
            System.currentTimeMillis(), System.currentTimeMillis() + 1000);

        // one run in each stopped state
        for (ProgramRunStatus runStatus : ProgramRunStatus.values()) {
          if (!runStatus.isEndState() || runStatus == ProgramRunStatus.REJECTED) {
            continue;
          }
          runId = programId.run(RunIds.generate());
          store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
          store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          twillRunId = UUID.randomUUID().toString();
          store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          store.recordProgramStop(runId, System.currentTimeMillis(), runStatus, null,
              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        }
      });
    }

    // test get number of active runs. It checks if the number of active runs per program, per namespace
    // is equal to 10. (2 versions each * 5 active states)
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      // check active runs per program reference
      for (ProgramReference programRef: programRefs) {
        int numActiveRuns = store.getProgramActiveRunsCount(programRef);
        Assert.assertEquals(10, numActiveRuns);
      }
    });
  }

  @Test
  public void testDuplicateWritesIgnored() throws Exception {
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.values()[ProgramType.values().length - 1],
                                            "program");
    ProgramRunId runId = program.run(RunIds.generate());

    byte[] sourceId = new byte[] { 0 };
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      assertSecondCallIsNull(() -> store.recordProgramProvisioning(runId, null, SINGLETON_PROFILE_MAP,
                                                                   sourceId, ARTIFACT_ID));
      assertSecondCallIsNull(() -> store.recordProgramProvisioned(runId, 0, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramStart(runId, null, Collections.emptyMap(), sourceId));
      assertSecondCallIsNull(() -> store.recordProgramRunning(runId, System.currentTimeMillis(), null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramSuspend(runId, sourceId, System.currentTimeMillis()));
      assertSecondCallIsNull(() -> store.recordProgramRunning(runId, System.currentTimeMillis(), null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramStop(runId, System.currentTimeMillis(), ProgramRunStatus.KILLED,
                                                           null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramDeprovisioning(runId, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramDeprovisioned(runId, System.currentTimeMillis(), sourceId));
    });
  }

  @Test
  public void testScanActiveRuns() {
    // Insert 10 Running programs
    List<ProgramRunId> runIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final ProgramRunId runId = new ProgramRunId("test", "test" + i, ProgramType.SERVICE, "service" + i,
                                                  RunIds.generate().getId());
      runIds.add(runId);

      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }
    // Insert 10 completed programs
    for (int i = 10; i < 20; i++) {
      final ProgramRunId runId = new ProgramRunId("test", "test" + i, ProgramType.SERVICE, "service" + i,
                                                  RunIds.generate().getId());
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStop(runId, System.currentTimeMillis(), ProgramRunStatus.COMPLETED, null,
                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    // Scan the active runs with batch size of 2
    AtomicReference<AppMetadataStore.Cursor> cursorRef = new AtomicReference<>(AppMetadataStore.Cursor.EMPTY);
    AtomicInteger count = new AtomicInteger();
    AtomicBoolean completed = new AtomicBoolean();
    while (!completed.get()) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        completed.set(true);
        store.scanActiveRuns(cursorRef.get(), 2, (cursor, runRecordDetail) -> {
          completed.set(false);
          cursorRef.set(cursor);
          Assert.assertEquals(runIds.get(count.get()), runRecordDetail.getProgramRunId());
          count.incrementAndGet();
        });
      });
    }

    // Scan from the last cursor should have empty result
    AtomicBoolean hasResult = new AtomicBoolean();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.scanActiveRuns(cursorRef.get(), 1, (cursor, runRecordDetail) -> {
        hasResult.set(true);
      });
    });

    Assert.assertFalse(hasResult.get());
  }

  private <T> void assertSecondCallIsNull(Callable<T> callable) throws Exception {
    T result = callable.call();
    Assert.assertNotNull(result);
    result = callable.call();
    Assert.assertNull(result);
  }

  @Test
  public void testProfileInRunRecord() throws Exception {
    ProgramRunId runId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram").run(RunIds.generate());
    ProfileId profileId = NamespaceId.DEFAULT.profile("MyProfile");

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      long startSourceId = 1L;
      store.recordProgramProvisioning(runId, null,
                                      Collections.singletonMap(SystemArguments.PROFILE_NAME, profileId.getScopedName()),
                                      AppFabricTestHelper.createSourceId(startSourceId), ARTIFACT_ID);
      // the profile id should be there after the provisioning stage
      RunRecordDetail run = store.getRun(runId);
      Assert.assertNotNull(run);
      Assert.assertEquals(profileId, run.getProfileId());

      store.recordProgramProvisioned(runId, 0, AppFabricTestHelper.createSourceId(startSourceId + 1));
      store.recordProgramStart(runId, null, ImmutableMap.of(),
                               AppFabricTestHelper.createSourceId(startSourceId + 2));
      store.recordProgramRunning(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS),
                                 null, AppFabricTestHelper.createSourceId(startSourceId + 3));
      store.recordProgramStop(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS),
                              ProgramRunStatus.KILLED,
                              null, AppFabricTestHelper.createSourceId(startSourceId + 4));
      run = store.getRun(runId);
      Assert.assertNotNull(run);
      Assert.assertEquals(profileId, run.getProfileId());
    });
  }

  @Test
  public void testOrderedActiveRuns() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("test").workflow("test");
    List<ProgramRunId> expectedRuns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      RunId runId = RunIds.generate(i * 1000);
      ProgramRunId run = programId.run(runId);
      expectedRuns.add(run);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        recordProvisionAndStart(run, store);
      });
    }
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Map<ProgramRunId, RunRecordDetail> activeRuns = store.getActiveRuns(programId);
      // the result should be sorted with larger time stamp come first
      Assert.assertEquals(Lists.reverse(expectedRuns), new ArrayList<>(activeRuns.keySet()));
    });
  }

  @Test
  public void testProgramRunCount() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("test").workflow("test");
    List<ProgramRunId> runIds = addProgramCount(programId, 5);

    // should have 5 runs
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Assert.assertEquals(5, store.getProgramRunCount(programId));
    });

    // stop all the program runs
    for (ProgramRunId runId : runIds) {
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.recordProgramRunning(
          runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS) + 10, null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStop(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS) + 20,
                                ProgramRunStatus.COMPLETED, null,
                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    // should still have 5 runs even we record stop of the program run
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Assert.assertEquals(5, store.getProgramRunCount(programId));
    });

    addProgramCount(programId, 3);

    // should have 8 runs
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Assert.assertEquals(8, store.getProgramRunCount(programId));
    });

    // after cleanup we should only have 0 runs
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.deleteProgramHistory(programId.getNamespace(), programId.getApplication(), programId.getVersion());
      Assert.assertEquals(0, store.getProgramRunCount(programId));
    });
  }

  @Test
  public void testBatchApplications() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Writes 20 application specs
    for (int i = 0; i < 20; i++) {
      String appName = "test" + i;
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(), appName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
        store.writeApplication(NamespaceId.SYSTEM.getNamespace(), appName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });
    }
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      long count =  store.getApplicationCount();
      // System apps are not included in the count
      Assert.assertEquals(20, count);
    });

    // Batch read 30, expect to get back 20
    List<ApplicationId> appIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      appIds.add(NamespaceId.DEFAULT.app("test" + i));
    }
    FunctionWithException<ApplicationId, SourceControlMeta, IOException> sourceControlRetriever
        = appId -> new SourceControlMeta("fileHash", "commitId", Instant.now());
    Map<ApplicationId, ApplicationMeta> result = TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      return store.getApplicationsForAppIds(appIds, sourceControlRetriever);
    });

    Assert.assertEquals(20, result.size());
    for (int i = 0; i < 20; i++) {
      Assert.assertTrue("Missing application test" + i, result.containsKey(NamespaceId.DEFAULT.app("test" + i)));
    }
  }

  @Test
  public void testScanApplications() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Writes 100 application specs
    int count = 100;
    for (int i = 0; i < count; i++) {
      String appName = "test" + i;
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(), appName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });
    }

    // Scan all apps
    Map<ApplicationId, ApplicationMeta> apps = new LinkedHashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder().build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });

    Assert.assertEquals(count, apps.size());
  }

    @Test
  public void testScanApplicationsReverse() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Writes 100 application specs
    int count = 100;
    for (int i = 0; i < count; i++) {
      String appName = "test" + i;
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(), appName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });
    }

    // Scan all apps
    Map<ApplicationId, ApplicationMeta> apps = new LinkedHashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder().build(),
                             entry -> {
          apps.put(entry.getKey(), entry.getValue());
          return true;
        });
    });

    Assert.assertEquals(count, apps.size());
    List<ApplicationId> appIds = new ArrayList<>(apps.keySet());
    List<ApplicationId> reverseIds = Lists.reverse(appIds);

    // Scan reverse
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder().setSortOrder(SortOrder.DESC).build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });

    Assert.assertEquals(reverseIds, new ArrayList<>(apps.keySet()));

    // Scan paged
    int pageSize = 5;
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder()
                               .setScanFrom(appIds.get(pageSize - 1)).build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });

    Assert.assertEquals(appIds.subList(pageSize, count), new ArrayList<>(apps.keySet()));

    // Scan paged reverse
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder()
                               .setSortOrder(SortOrder.DESC)
                               .setScanFrom(reverseIds.get(pageSize - 1)).build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });

    Assert.assertEquals(reverseIds.subList(pageSize, count), new ArrayList<>(apps.keySet()));

    // Scan by batches
    apps.clear();
    {
      AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(
        ScanApplicationsRequest.builder().build());
      for (int i = 0; i <= count / 15; i++) {
        TransactionRunners.run(transactionRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          store.scanApplications(requestRef.get(), entry -> {
            apps.put(entry.getKey(), entry.getValue());
            ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
              .builder(requestRef.get()).setScanFrom(entry.getKey()).build();
            requestRef.set(nextBatchRequest);
            return apps.size() % 15 != 0;
          });
        });
      }
    }
    Assert.assertEquals(appIds, new ArrayList<>(apps.keySet()));

    // Scan paged by batches
    apps.clear();
    {
      AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(
        ScanApplicationsRequest.builder()
          .setScanFrom(appIds.get(pageSize - 1))
          .build());
      for (int i = 0; i <= count / 15; i++) {
        TransactionRunners.run(transactionRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          store.scanApplications(requestRef.get(), entry -> {
            apps.put(entry.getKey(), entry.getValue());
            ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
              .builder(requestRef.get()).setScanFrom(entry.getKey()).build();
            requestRef.set(nextBatchRequest);
            return apps.size() % 15 != 0;
          });
        });
      }
    }
    Assert.assertEquals(appIds.subList(pageSize, count), new ArrayList<>(apps.keySet()));
  }

  @Test
  public void testScanApplicationsWithNamespace() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Writes 100 application specs
    int count = 100;
    for (int i = 0; i < count / 2; i++) {
      String defaultAppName = "test" + (2 * i);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(),
                               defaultAppName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });

      String cdapAppName = "test" + (2 * i + 1);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.CDAP.getNamespace(),
                               cdapAppName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });
    }

    // Scan all apps
    Map<ApplicationId, ApplicationMeta> apps = new LinkedHashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder().setNamespaceId(NamespaceId.DEFAULT).build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });
    Assert.assertEquals(count / 2, apps.size());
  }

  @Test
  public void testScanApplicationsWithNamespaceReverse() {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Writes 100 application specs
    int count = 100;
    for (int i = 0; i < count / 2; i++) {
      String defaultAppName = "test" + (2 * i);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(),
            defaultAppName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });

      String cdapAppName = "test" + (2 * i + 1);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        store.writeApplication(NamespaceId.CDAP.getNamespace(),
            cdapAppName, ApplicationId.DEFAULT_VERSION, appSpec,
                               new ChangeDetail(null, null, null, creationTimeMillis), null);
      });
    }

    // Scan all apps
    Map<ApplicationId, ApplicationMeta> apps = new LinkedHashMap<>();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder().setNamespaceId(NamespaceId.DEFAULT).build(),
                             entry -> {
          apps.put(entry.getKey(), entry.getValue());
          return true;
        });
    });
    Assert.assertEquals(count / 2, apps.size());
    List<ApplicationId> appIds = new ArrayList<>(apps.keySet());
    List<ApplicationId> reverseIds = Lists.reverse(appIds);

    // Scan reverse
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder()
                               .setNamespaceId(NamespaceId.DEFAULT)
                               .setSortOrder(SortOrder.DESC)
                               .build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });
    Assert.assertEquals(reverseIds, new ArrayList<>(apps.keySet()));

    // Scan paged
    int pageSize = 5;
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder()
                               .setNamespaceId(NamespaceId.DEFAULT)
                               .setScanFrom(appIds.get(pageSize - 1))
                               .build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });

    Assert.assertEquals(appIds.subList(pageSize, count / 2), new ArrayList<>(apps.keySet()));

    // Scan paged reverse
    apps.clear();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);

      store.scanApplications(ScanApplicationsRequest.builder()
                               .setNamespaceId(NamespaceId.DEFAULT)
                               .setSortOrder(SortOrder.DESC)
                               .setScanFrom(reverseIds.get(pageSize - 1))
                               .build(),
                             entry -> {
                               apps.put(entry.getKey(), entry.getValue());
                               return true;
                             });
    });
    Assert.assertEquals(reverseIds.subList(pageSize, count / 2), new ArrayList<>(apps.keySet()));

    // Scan by batches
    apps.clear();
    {
      AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(
        ScanApplicationsRequest.builder().setNamespaceId(NamespaceId.DEFAULT).build());
      for (int i = 0; i <= count / 15; i++) {
        TransactionRunners.run(transactionRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          store.scanApplications(requestRef.get(),
                                 entry -> {
                                   apps.put(entry.getKey(), entry.getValue());
                                   ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
                                     .builder(requestRef.get()).setScanFrom(entry.getKey()).build();
                                   requestRef.set(nextBatchRequest);
                                   return apps.size() % 15 != 0;
                                 });
        });
      }
    }
    Assert.assertEquals(appIds, new ArrayList<>(apps.keySet()));

    // Scan reverse by batches
    apps.clear();
    {
      AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(
        ScanApplicationsRequest.builder()
          .setNamespaceId(NamespaceId.DEFAULT)
          .setSortOrder(SortOrder.DESC)
          .build());
      for (int i = 0; i <= count / 15; i++) {
        TransactionRunners.run(transactionRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          store.scanApplications(requestRef.get(),
                                 entry -> {
                                   apps.put(entry.getKey(), entry.getValue());
                                   ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
                                     .builder(requestRef.get()).setScanFrom(entry.getKey()).build();
                                   requestRef.set(nextBatchRequest);
                                   return apps.size() % 15 != 0;
                                 });
        });
      }
    }
    Assert.assertEquals(reverseIds, new ArrayList<>(apps.keySet()));

    // Scan paged by batches
    apps.clear();
    {
      AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(
        ScanApplicationsRequest.builder()
          .setNamespaceId(NamespaceId.DEFAULT)
          .setScanFrom(appIds.get(pageSize - 1))
          .build());
      for (int i = 0; i <= count / 15; i++) {
        TransactionRunners.run(transactionRunner, context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          store.scanApplications(requestRef.get(), entry -> {
            apps.put(entry.getKey(), entry.getValue());
            ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
              .builder(requestRef.get()).setScanFrom(entry.getKey()).build();
            requestRef.set(nextBatchRequest);
            return apps.size() % 15 != 0;
          });
        });
      }
    }
    Assert.assertEquals(appIds.subList(pageSize, count / 2), new ArrayList<>(apps.keySet()));
  }

  @Test
  public void testBatchProgramRunCount() throws Exception {
    ProgramId programId1 = NamespaceId.DEFAULT.app("test").workflow("test1");
    ProgramId programId2 = NamespaceId.DEFAULT.app("test").workflow("test2");
    ProgramId programId3 = NamespaceId.DEFAULT.app("test").workflow("test3");

    // add some run records to program1 and 2
    addProgramCount(programId1, 5);
    addProgramCount(programId2, 3);

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Map<ProgramReference, Long> counts =
        store.getProgramTotalRunCounts(ImmutableList.of(programId1.getProgramReference(),
                                                        programId2.getProgramReference(),
                                                        programId3.getProgramReference()));
      Assert.assertEquals(5, (long) counts.get(programId1.getProgramReference()));
      Assert.assertEquals(3, (long) counts.get(programId2.getProgramReference()));
      Assert.assertEquals(0, (long) counts.get(programId3.getProgramReference()));
    });

    // after cleanup we should only have 0 runs for all programs
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.deleteProgramHistory(programId1.getNamespace(), programId1.getApplication(), programId1.getVersion());
      store.deleteProgramHistory(programId2.getNamespace(), programId2.getApplication(), programId2.getVersion());
      store.deleteProgramHistory(programId3.getNamespace(), programId3.getApplication(), programId3.getVersion());
    });

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      Map<ProgramReference, Long> counts =
        store.getProgramTotalRunCounts(ImmutableList.of(programId1.getProgramReference(),
                                                        programId2.getProgramReference(),
                                                        programId3.getProgramReference()));
      Assert.assertEquals(0, (long) counts.get(programId1.getProgramReference()));
      Assert.assertEquals(0, (long) counts.get(programId2.getProgramReference()));
      Assert.assertEquals(0, (long) counts.get(programId3.getProgramReference()));
    });
  }

  @Test
  public void testConcurrentCreateAppFirstVersion() throws Exception {
    String appName = "application1";
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ApplicationReference appRef = new ApplicationReference(NamespaceId.DEFAULT, appName);

    // Concurrently deploy different fist version of the same application
    int numThreads = 10;
    AtomicInteger idGenerator = new AtomicInteger();
    runConcurrentOperation("concurrent-first-deploy-application", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore metaStore = AppMetadataStore.create(context);
        int id = idGenerator.getAndIncrement();
        ApplicationId appId = appRef.app(appName + "_version_" + id);
        ApplicationSpecification spec = createDummyAppSpec(appId.getApplication(), appId.getVersion(), artifactId);
        ApplicationMeta meta = new ApplicationMeta(spec.getName(), spec,
                                                   new ChangeDetail(null, null, null,
                                                                    creationTimeMillis + id));
        metaStore.createLatestApplicationVersion(appId, meta);
      })
    );

    // Verify latest version
    AtomicInteger latestVersionCount = new AtomicInteger();
    AtomicInteger allVersionsCount = new AtomicInteger();
    AtomicInteger appEditNumber = new AtomicInteger();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = AppMetadataStore.create(context);
      List<ApplicationMeta> allVersions = new ArrayList<>();
      metaStore.scanApplications(
        ScanApplicationsRequest.builder().setApplicationReference(appRef).build(),
        entry -> {
          allVersions.add(entry.getValue());
          return true;
        });

      List<String> latestVersions = allVersions
        .stream()
        .filter(version -> {
          Assert.assertNotNull(version.getChange());
          Assert.assertNotNull(version.getChange().getLatest());
          return version.getChange().getLatest().equals(true);
        })
        .map(version -> version.getSpec().getAppVersion())
        .collect(Collectors.toList());
      allVersionsCount.set(allVersions.size());
      latestVersionCount.set(latestVersions.size());
      appEditNumber.set(metaStore.getApplicationEditNumber(appRef));
    });

    // There can only be one latest version
    Assert.assertEquals(1, latestVersionCount.get());
    Assert.assertEquals(numThreads, allVersionsCount.get());
    Assert.assertEquals(numThreads, appEditNumber.get());
  }

  @Test
  public void testConcurrentCreateAppAfterTheFirstVersion() throws Exception {
    String appName = "application1";
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ApplicationReference appRef = new ApplicationReference(NamespaceId.DEFAULT, appName);

    AtomicInteger idGenerator = new AtomicInteger();
    // Deploy the first version
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = AppMetadataStore.create(context);
      int id = idGenerator.getAndIncrement();
      ApplicationId appId = appRef.app(appName + "_version_" + id);
      ApplicationSpecification spec = createDummyAppSpec(appId.getApplication(), appId.getVersion(), artifactId);
      ApplicationMeta meta = new ApplicationMeta(spec.getName(), spec,
                                                 new ChangeDetail(null, null, null,
                                                                  creationTimeMillis + id));
      metaStore.createLatestApplicationVersion(appId, meta);
    });

    // Concurrently deploy different versions of the same application
    int numThreads = 10;
    runConcurrentOperation("concurrent-second-deploy-application", numThreads, () ->
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore metaStore = AppMetadataStore.create(context);
        int id = idGenerator.getAndIncrement();
        ApplicationId appId = appRef.app(appName + "_version_" + id);
        ApplicationSpecification spec = createDummyAppSpec(appId.getApplication(), appId.getVersion(), artifactId);
        ApplicationMeta meta = new ApplicationMeta(spec.getName(), spec,
                                                   new ChangeDetail(null, null, null,
                                                                    creationTimeMillis + id));
        metaStore.createLatestApplicationVersion(appId, meta);
      })
    );

    // Verify latest version
    AtomicInteger latestVersionCount = new AtomicInteger();
    AtomicInteger allVersionsCount = new AtomicInteger();
    AtomicInteger appEditNumber = new AtomicInteger();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = AppMetadataStore.create(context);
      List<ApplicationMeta> allVersions = new ArrayList<>();
      metaStore.scanApplications(
        ScanApplicationsRequest.builder().setApplicationReference(appRef).build(),
        entry -> {
          allVersions.add(entry.getValue());
          return true;
        });
      List<String> latestVersions = allVersions
        .stream()
        .filter(version -> {
          Assert.assertNotNull(version.getChange());
          Assert.assertNotNull(version.getChange().getLatest());
          return version.getChange().getLatest().equals(true);
        })
        .map(version -> version.getSpec().getAppVersion())
        .collect(Collectors.toList());
      allVersionsCount.set(allVersions.size());
      latestVersionCount.set(latestVersions.size());
      appEditNumber.set(metaStore.getApplicationEditNumber(appRef));
    });

    // There can only be one latest version
    Assert.assertEquals(1, latestVersionCount.get());
    Assert.assertEquals(1 + numThreads, allVersionsCount.get());
    Assert.assertEquals(1 + numThreads, appEditNumber.get());
  }

  @Test
  public void testDeleteCompletedRunsStartedBefore() throws Exception {
    // Map an iterator to one of 15 different program+workflow permutations. Used to ensure
    // (1) Multiple types of ProgramId exist (2) Multiple runs exist for each ProgramId.
    IntFunction<ProgramId> getProgramId =
        (int i) ->
            NamespaceId.DEFAULT
                .app(String.format("test%d", i % 3))
                .workflow(String.format("test%d", i % 5));

    Instant ttlCutoff = Instant.ofEpochSecond(1631629389);

    Set<ProgramRunId> shouldExpire =
        IntStream.range(1, 101)
            .mapToObj(i -> createCompletedRun(getProgramId.apply(i), ttlCutoff.minusSeconds(i)))
            .collect(Collectors.toSet());
    Set<ProgramRunId> shouldNotExpire =
        IntStream.range(1, 101)
            .mapToObj(i -> createCompletedRun(getProgramId.apply(i), ttlCutoff.plusSeconds(i)))
            .collect(Collectors.toSet());

    TransactionRunners.run(
        transactionRunner,
        context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          // Check that run data exists for both job sets initially.
          store.getRuns(shouldExpire).values().forEach(Assert::assertNotNull);
          store.getRuns(shouldNotExpire).values().forEach(Assert::assertNotNull);

          // Run expiration process.
          store.deleteCompletedRunsStartedBefore(ttlCutoff);
        });

    TransactionRunners.run(
        transactionRunner,
        context -> {
          AppMetadataStore store = AppMetadataStore.create(context);

          // Assert only the older runs were deleted.
          store.getRuns(shouldExpire).values().forEach(Assert::assertNull);
          store.getRuns(shouldNotExpire).values().forEach(Assert::assertNotNull);
        });
  }

  /**
   * Testcase for getting the latest application, where the application was deployed
   * before 6.8.0 (where the latest column is not set).
   * In this case, first insert a row in app spec table with the latest column set to null.
   * This step is expected to fail in the NoSql implementation.
   */
  @Test
  public void testGetLatestOnLegacyRows() throws Exception {
    Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
    // insert a row in appspec table with latest column set to null
    String appName = "legacy_app_without_latest";
    String appVersion = ApplicationId.DEFAULT_VERSION;
    ApplicationReference appRef = new ApplicationReference(NamespaceId.DEFAULT, appName);

    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ApplicationId appId = appRef.app(appVersion);
    ApplicationSpecification spec = createDummyAppSpec(appId.getApplication(), appId.getVersion(), artifactId);
    ApplicationMeta appMeta = new ApplicationMeta(appName, spec, null, null);

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = AppMetadataStore.create(context);
      metaStore.createLatestApplicationVersion(appId, appMeta);
      StructuredTable appSpecTable = context.getTable(
          StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS);

      List<Field<?>> fields = metaStore.getApplicationPrimaryKeys(
          NamespaceId.DEFAULT.getNamespace(), appName, appVersion);
      fields.add(Fields.booleanField(StoreDefinition.AppMetadataStore.LATEST_FIELD, null));
      appSpecTable.upsert(fields);
    });

    ApplicationMeta latestAppMeta = TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = AppMetadataStore.create(context);
      return metaStore.getLatest(appRef);
    });

    Assert.assertEquals(appName, latestAppMeta.getId());
    Assert.assertEquals(appVersion, latestAppMeta.getSpec().getAppVersion());
  }

  /**
   * Creates a new run of {@code programRunId} in the completed state with a starting time of {@code
   * startingTime} and returns its corresponding run id.
   *
   * <p>The job will enter the following states: Provisioning -> Provisioned -> Starting -> Running
   * -> Completed
   *
   * <p>The "start" and "end" times for the returned RunId will be after the provided "starting"
   * time.
   */
  private ProgramRunId createCompletedRun(ProgramId programId, Instant startingTime) {
    RunId runId = RunIds.generate(startingTime.toEpochMilli());
    ProgramRunId run = programId.run(runId);

    TransactionRunners.run(
        transactionRunner,
        context -> {
          AppMetadataStore store = AppMetadataStore.create(context);
          recordProvisionAndStart(run, store);
          store.recordProgramRunning(
              run,
              startingTime.plusSeconds(10).getEpochSecond(),
              null,
              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          store.recordProgramStop(
              run,
              startingTime.plusSeconds(20).getEpochSecond(),
              ProgramRunStatus.COMPLETED,
              null,
              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        });
    return run;
  }

  /**
   * Adds {@code count} new runs of {@code programRunId} in the completed state, returning their
   * corresponding run ids.
   */
  private List<ProgramRunId> addProgramCount(ProgramId programId, int count) throws Exception {
    Instant startTimeBase = Instant.ofEpochSecond(0);
    List<ProgramRunId> runIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      runIds.add(createCompletedRun(programId, startTimeBase.plusSeconds(i)));
    }
    return runIds;
  }

  private ApplicationSpecification createDummyAppSpec(String appName, String appVersion, ArtifactId artifactId) {
    return new DefaultApplicationSpecification(
      appName, appVersion, ProjectInfo.getVersion().toString(), "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
  }

  private void runConcurrentOperation(String name, int numThreads, Runnable runnable) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    try {
      for (int i = 0; i < numThreads; ++i) {
        executorService.submit(() -> {
          try {
            startLatch.await();
            runnable.run();
            doneLatch.countDown();
          } catch (Exception e) {
            LOG.error("Error performing concurrent operation {}", name, e);
          }
        });
      }

      startLatch.countDown();
      doneLatch.await(30, TimeUnit.SECONDS);
    } finally {
      executorService.shutdown();
    }
  }
}
