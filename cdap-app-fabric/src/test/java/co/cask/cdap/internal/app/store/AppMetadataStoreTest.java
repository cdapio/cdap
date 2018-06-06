/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test AppMetadataStore.
 */
public class AppMetadataStoreTest {
  private static DatasetFramework datasetFramework;
  private static CConfiguration cConf;
  private static TransactionExecutorFactory txExecutorFactory;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();

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

  private void recordProvisionAndStart(ProgramRunId programRunId, AppMetadataStore metadataStoreDataset) {
    metadataStoreDataset.recordProgramProvisioning(programRunId, null, new HashMap<>(),
                                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                   ARTIFACT_ID);
    metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
    metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
  }

  // TODO: [CDAP-12458] since recordProgramStart doesn't use version-less key builder, this test fails.
  @Ignore
  @Test
  public void testOldRunRecordFormat() throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset("testOldRunRecordFormat");
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, Collections.emptyMap(), null);
    Assert.assertNotNull(table);
    final AppMetadataStore metadataStoreDataset = new AppMetadataStore(table, cConf);
    TransactionExecutor txnl = txExecutorFactory.createExecutor(
      Collections.singleton(metadataStoreDataset));

    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.values()[ProgramType.values().length - 1],
                                                  "program");
    final RunId runId = RunIds.generate();
    final ProgramRunId programRunId = program.run(runId);
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunningOldFormat(
        programRunId, RunIds.getTime(runId, TimeUnit.SECONDS), null,
        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
    });

    txnl.execute(() -> {
      Set<RunId> runIds = metadataStoreDataset.getRunningInRange(0, Long.MAX_VALUE);
      Assert.assertEquals(1, runIds.size());
      RunRecordMeta meta = metadataStoreDataset.getRun(program.run(runIds.iterator().next().getId()));
      Assert.assertNotNull(meta);
      Assert.assertEquals(runId.getId(), meta.getPid());
    });

    txnl.execute(() -> {
      metadataStoreDataset.recordProgramStopOldFormat(
        programRunId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
        ProgramRunStatus.COMPLETED, null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      Map<ProgramRunId, RunRecordMeta> runRecordMap = metadataStoreDataset.getRuns(
        program, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, Integer.MAX_VALUE, null);
      Assert.assertEquals(1, runRecordMap.size());
      ProgramRunId fetchedRunId = runRecordMap.keySet().iterator().next();
      Assert.assertEquals(programRunId, fetchedRunId);
    });
  }

  private AppMetadataStore getMetadataStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.of(), null);
    Assert.assertNotNull(table);
    return new AppMetadataStore(table, cConf);
  }

  private TransactionExecutor getTxExecutor(AppMetadataStore metadataStoreDataset) {
    return txExecutorFactory.createExecutor(Collections.singleton(metadataStoreDataset));
  }

  @Test
  public void testSmallerSourceIdRecords() throws Exception {
    AppMetadataStore metadataStoreDataset = getMetadataStore("testSmallerSourceIdRecords");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    // STARTING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 100L, 10L, 1L, ProgramRunStatus.STARTING);
    assertPersistedStatus(metadataStoreDataset, txnl, 100L, 1L, 10L, ProgramRunStatus.STARTING);
    // RUNNING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 1L, 100L, 10L, ProgramRunStatus.RUNNING);
    // KILLED status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 1L, 10L, 100L, ProgramRunStatus.KILLED);
  }

  @Test
  public void testProvisioningFailure() throws Exception {
    AppMetadataStore metadataStoreDataset = getMetadataStore("testProvisioningFailure");

    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    RunId runId1 = RunIds.generate();
    ProgramRunId programRunId1 = program.run(runId1);

    // test state transition from provisioning -> deprovisioning. This can happen if there is a provisioning failure
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramProvisioning(programRunId1, Collections.emptyMap(), Collections.emptyMap(),
                                                     AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                     ARTIFACT_ID);
      metadataStoreDataset.recordProgramDeprovisioning(programRunId1,
                                                       AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
      Assert.assertEquals(ProgramRunClusterStatus.DEPROVISIONING, runRecordMeta.getCluster().getStatus());
    });

    RunId runId2 = RunIds.generate();
    ProgramRunId programRunId2 = program.run(runId2);
    // test state transition from provisioning -> deprovisioned. This can happen if there is a provisioning failure
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramProvisioning(programRunId2, Collections.emptyMap(), Collections.emptyMap(),
                                                     AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                     ARTIFACT_ID);
      metadataStoreDataset.recordProgramDeprovisioned(programRunId2,
                                                      AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId2);
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
      Assert.assertEquals(ProgramRunClusterStatus.DEPROVISIONED, runRecordMeta.getCluster().getStatus());
    });
  }

  @Test
  public void testInvalidStatusPersistence() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testInvalidStatusPersistence");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId1 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId1 = program.run(runId1);
    final AtomicLong sourceId = new AtomicLong();
    // No status can be persisted if STARTING is not present
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramRunning(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId2 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId2 = program.run(runId2);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramSuspend(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      metadataStoreDataset.recordProgramResumed(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId2);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId3 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId3 = program.run(runId3);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId3);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId4 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId4 = program.run(runId4);
    // Once a stop status is reached, any incoming status will be ignored
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId4, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId4);
      // KILLED after COMPLETED is ignored
      Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecordMeta.getStatus());
    });
    final RunId runId5 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId5 = program.run(runId5);
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId5, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId5);
      // COMPLETED after FAILED is ignored
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
    });
    final RunId runId6 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId6 = program.run(runId6);
    Long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // STARTING status will be ignored if there's any existing record
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId6, metadataStoreDataset);
      metadataStoreDataset.recordProgramSuspend(programRunId6,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                currentTime);
      metadataStoreDataset.recordProgramStart(programRunId6, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId6);
      // STARTING status is ignored since there's an existing SUSPENDED record
      Assert.assertEquals(ProgramRunStatus.SUSPENDED, runRecordMeta.getStatus());
      Assert.assertEquals(currentTime, runRecordMeta.getSuspendTs());
    });
    final RunId runId7 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId7 = program.run(runId7);
    txnl.execute(() -> {
      long startTime = RunIds.getTime(runId7, TimeUnit.SECONDS);
      recordProvisionAndStart(programRunId7, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunning(programRunId7, startTime, null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStart(programRunId7, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId7);
      // STARTING status is ignored since there's an existing RUNNING record
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMeta.getStatus());
    });
  }

  private void assertPersistedStatus(final AppMetadataStore metadataStoreDataset, TransactionExecutor txnl,
                                     final long startSourceId, final long runningSourceId,
                                     final long killedSourceId, final ProgramRunStatus expectedRunStatus)
    throws Exception {
    // Add some run records
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final AtomicReference<RunRecordMeta> resultRecord = new AtomicReference<>();
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramProvisioning(programRunId, null, new HashMap<>(),
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
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testScanRunningInRange");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);

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
      txnl.execute(() -> {
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
    runScan(txnl, metadataStoreDataset, expected, 0, Long.MAX_VALUE);

    // In all below assertions, TreeSet and metadataStore both have start time inclusive and end time exclusive.
    // Run the scan with time limit
    runScan(txnl, metadataStoreDataset, expected.subSet(30 * 10000L, 90 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(90 * 10000));

    runScan(txnl, metadataStoreDataset, expected.subSet(90 * 10000L, 101 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(90 * 10000), TimeUnit.MILLISECONDS.toSeconds(101 * 10000));

    // After range
    runScan(txnl, metadataStoreDataset, expected.subSet(101 * 10000L, 200 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(101 * 10000), TimeUnit.MILLISECONDS.toSeconds(200 * 10000));

    // Identical start and end time
    runScan(txnl, metadataStoreDataset, expected.subSet(31 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(31 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // One unit difference between start and end time
    runScan(txnl, metadataStoreDataset, expected.subSet(30 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // Before range
    runScan(txnl, metadataStoreDataset, expected.subSet(1000L, 10000L),
            TimeUnit.MILLISECONDS.toSeconds(1000), TimeUnit.MILLISECONDS.toSeconds(10000));
  }

  private void runScan(TransactionExecutor txnl, final AppMetadataStore metadataStoreDataset,
                       final Set<Long> expected, final long startTime, final long stopTime)
    throws InterruptedException, TransactionFailureException {
    txnl.execute(() -> {
      // Run the scan
      Set<Long> actual = new TreeSet<>();
      int maxScanTimeMillis = 25;
      // Create a ticker that counts one millisecond per element, so that we can test batching
      // Hence number of elements per batch = maxScanTimeMillis
      CountingTicker countingTicker = new CountingTicker(1);
      List<Iterable<RunId>> batches =
        metadataStoreDataset.getRunningInRangeForStatus("runRecordCompleted", startTime, stopTime, maxScanTimeMillis,
                                                        countingTicker);
      Iterable<RunId> runIds = Iterables.concat(batches);
      Iterables.addAll(actual, Iterables.transform(runIds, input -> RunIds.getTime(input, TimeUnit.MILLISECONDS)));

      Assert.assertEquals(expected, actual);
      int numBatches = Iterables.size(batches);
      // Each batch needs 2 extra calls to Ticker.read, once during init and once for final condition check
      // Hence the number of batches should be --
      // (num calls to Ticker.read - (2 * numBatches)) / number of elements per batch
      Assert.assertEquals((countingTicker.getNumProcessed() - (2 * numBatches)) / maxScanTimeMillis, numBatches);
    });
  }

  @Test
  public void testgetRuns() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testgetRuns");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);

    // Add some run records
    final Set<String> expected = new TreeSet<>();
    final Set<String> expectedHalf = new TreeSet<>();
    final Set<ProgramRunId> programRunIdSet = new HashSet<>();
    final Set<ProgramRunId> programRunIdSetHalf = new HashSet<>();
    for (int i = 0; i < 100; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app");
      final ProgramId program = application.program(ProgramType.FLOW, "program");
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
      txnl.execute(() -> {
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

    txnl.execute(() -> {
      Map<ProgramRunId, RunRecordMeta> runMap = metadataStoreDataset.getRuns(programRunIdSet);
      Set<String> actual = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordMeta> entry : runMap.entrySet()) {
        actual.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expected, actual);


      Map<ProgramRunId, RunRecordMeta> runMapHalf = metadataStoreDataset.getRuns(programRunIdSetHalf);
      Set<String> actualHalf = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordMeta> entry : runMapHalf.entrySet()) {
        actualHalf.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expectedHalf, actualHalf);
    });
  }

  @Test
  public void testGetActiveRuns() throws Exception {
    AppMetadataStore store = getMetadataStore("testGetActiveRuns");
    TransactionExecutor txnl = getTxExecutor(store);

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
      txnl.execute(() -> {
        // one run in pending state
        ProgramRunId runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);

        // one run in starting state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStart(runId, UUID.randomUUID().toString(), Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one run in running state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one in suspended state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
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
          store.recordProgramProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
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
    // check active runs per namespace
    for (NamespaceId namespace : namespaces) {
      txnl.execute(() -> {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(namespace);

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
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
          ProgramId programId = activeRun.getKey().getParent();
          Assert.assertTrue("Unexpected program returned: " + programId,
                            actual.containsKey(activeRun.getKey().getParent()));
          actual.get(programId).add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(expected, actual);
      });
    }

    // check active runs per app
    for (ApplicationId app : apps) {
      txnl.execute(() -> {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(app);

        // we expect 3 runs per program, with 2 programs in each app
        Map<ProgramId, Set<ProgramRunStatus>> expected = new HashMap<>();
        expected.put(app.mr(program1), activeStates);
        expected.put(app.mr(program2), activeStates);

        Map<ProgramId, Set<ProgramRunStatus>> actual = new HashMap<>();
        actual.put(app.mr(program1), new HashSet<>());
        actual.put(app.mr(program2), new HashSet<>());
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
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
      txnl.execute(() -> {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(program);

        Set<ProgramRunStatus> actual = new HashSet<>();
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
          Assert.assertEquals(program, activeRun.getKey().getParent());
          actual.add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(activeStates, actual);
      });
    }
  }

  private static class CountingTicker extends Ticker {
    private final long elementsPerMillis;
    private int numProcessed = 0;

    CountingTicker(long elementsPerMillis) {
      this.elementsPerMillis = elementsPerMillis;
    }

    int getNumProcessed() {
      return numProcessed;
    }

    @Override
    public long read() {
      ++numProcessed;
      return TimeUnit.MILLISECONDS.toNanos(numProcessed / elementsPerMillis);
    }
  }
}
