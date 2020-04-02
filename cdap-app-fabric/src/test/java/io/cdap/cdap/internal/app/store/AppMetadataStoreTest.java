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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test AppMetadataStore.
 */
public abstract class AppMetadataStoreTest {
  protected static TransactionRunner transactionRunner;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final Map<String, String> SINGLETON_PROFILE_MAP =
    Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());

  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong();

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
      Set<Long> actual = new TreeSet<>();
      Set<RunId> batches =
        metadataStoreDataset.getRunningInRangeForStatus("runRecordCompleted", startTime, stopTime);
      Iterables.addAll(actual, Iterables.transform(batches, input -> RunIds.getTime(input, TimeUnit.MILLISECONDS)));

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
    AtomicReference<AppMetadataStore.Cursor> cursorRef = new AtomicReference<>();
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
        store.writeApplication(NamespaceId.DEFAULT.getNamespace(), appName, ApplicationId.DEFAULT_VERSION, appSpec);
      });
    }

    // Batch read 30, expect to get back 20
    List<ApplicationId> appIds = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      appIds.add(NamespaceId.DEFAULT.app("test" + i));
    }
    Map<ApplicationId, ApplicationMeta> result = TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      return store.getApplicationsForAppIds(appIds);
    });

    Assert.assertEquals(20, result.size());
    for (int i = 0; i < 20; i++) {
      Assert.assertTrue("Missing application test" + i, result.containsKey(NamespaceId.DEFAULT.app("test" + i)));
    }
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
      Map<ProgramId, Long> counts = store.getProgramRunCounts(ImmutableList.of(programId1, programId2, programId3));
      Assert.assertEquals(5, (long) counts.get(programId1));
      Assert.assertEquals(3, (long) counts.get(programId2));
      Assert.assertEquals(0, (long) counts.get(programId3));
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
      Map<ProgramId, Long> counts = store.getProgramRunCounts(ImmutableList.of(programId1, programId2, programId3));
      Assert.assertEquals(0, (long) counts.get(programId1));
      Assert.assertEquals(0, (long) counts.get(programId2));
      Assert.assertEquals(0, (long) counts.get(programId3));
    });
  }

  private List<ProgramRunId> addProgramCount(ProgramId programId, int count) throws Exception {
    List<ProgramRunId> runIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RunId runId = RunIds.generate(i * 1000);
      ProgramRunId run = programId.run(runId);
      runIds.add(run);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        recordProvisionAndStart(run, store);
      });
    }
    return runIds;
  }
}
