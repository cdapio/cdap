/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Function;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Test AppMetadataStore.
 */
public class AppMetadataStoreTest {
  private static DatasetFramework datasetFramework;
  private static CConfiguration cConf;
  private static TransactionExecutorFactory txExecutorFactory;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);

  private final AtomicInteger sourceId = new AtomicInteger();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    datasetFramework = injector.getInstance(DatasetFramework.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    cConf = injector.getInstance(CConfiguration.class);
  }

  // TODO: [CDAP-12458] since recordProgramStart doesn't use version-less key builder, this test fails.
  @Ignore
  @Test
  public void testOldRunRecordFormat() throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset("testOldRunRecordFormat");
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.<String, String>of(), null);
    Assert.assertNotNull(table);
    final AppMetadataStore metadataStoreDataset = new AppMetadataStore(table, cConf, new AtomicBoolean(false));
    TransactionExecutor txnl = txExecutorFactory.createExecutor(
      Collections.singleton((TransactionAware) metadataStoreDataset));

    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.values()[ProgramType.values().length - 1],
                                                  "program");
    final RunId runId = RunIds.generate();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metadataStoreDataset.recordProgramStart(program, runId.getId(),
                                                RunIds.getTime(runId, TimeUnit.SECONDS), null, null, null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        metadataStoreDataset.recordProgramRunningOldFormat(
          program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS), null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Set<RunId> runIds = metadataStoreDataset.getRunningInRange(0, Long.MAX_VALUE);
        Assert.assertEquals(1, runIds.size());
        RunRecordMeta meta = metadataStoreDataset.getRun(program, runIds.iterator().next().getId());
        Assert.assertNotNull(meta);
        Assert.assertEquals(runId.getId(), meta.getPid());
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metadataStoreDataset.recordProgramStopOldFormat(
          program, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
          ProgramRunStatus.COMPLETED, null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        Map<ProgramRunId, RunRecordMeta> runRecordMap = metadataStoreDataset.getRuns(
          program, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, Integer.MAX_VALUE, null);
        Assert.assertEquals(1, runRecordMap.size());
        ProgramRunId programRunId = runRecordMap.keySet().iterator().next();
        Assert.assertEquals(program, programRunId.getParent());
        Assert.assertEquals(runId.getId(), programRunId.getRun());
      }
    });
  }

  private AppMetadataStore getMetadataStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.<String, String>of(), null);
    Assert.assertNotNull(table);
    return new AppMetadataStore(table, cConf, new AtomicBoolean(false));
  }

  private TransactionExecutor getTxExecutor(AppMetadataStore metadataStoreDataset) {
    return txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) metadataStoreDataset));
  }

  @Test
  public void testSmallerSourceIdRecords() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testSmallerSourceIdRecords");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    // STARTING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 1000L, 100L, 10L, ProgramRunStatus.STARTING);
    // RUNNING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 10L, 1000L, 10L, ProgramRunStatus.RUNNING);
    // KILLED status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 10L, 100L, 1000L, ProgramRunStatus.KILLED);
    // STARTING status is persisted since no following sourceId is larger
    assertPersistedStatus(metadataStoreDataset, txnl, 10L, 10L, 10L, ProgramRunStatus.STARTING);
  }

  @Test
  public void testInvalidStatusPersistence() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testInvalidStatusPersistence");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    // No status can be persisted if STARTING is not present
    assertPersistedStatus(metadataStoreDataset, txnl, null,
                          ImmutableList.of(ProgramRunStatus.RUNNING, ProgramRunStatus.COMPLETED));
    assertPersistedStatus(metadataStoreDataset, txnl, null,
                          ImmutableList.of(ProgramRunStatus.SUSPENDED, ProgramRunStatus.RESUMING));
    assertPersistedStatus(metadataStoreDataset, txnl, null, STOP_STATUSES);
    // Once a stop status is reached, any incoming status will be ignored
    for (int i = 0; i < STOP_STATUSES.size(); i++) {
      // A different stop status at the end will be ignored
      assertPersistedStatus(metadataStoreDataset, txnl, STOP_STATUSES.get(i),
                            ImmutableList.of(ProgramRunStatus.STARTING, STOP_STATUSES.get(i),
                                             STOP_STATUSES.get((i + 1) % STOP_STATUSES.size())));
    }
    // STARTING status will be ignored if there's any existing record
    assertPersistedStatus(metadataStoreDataset, txnl, ProgramRunStatus.SUSPENDED,
                          ImmutableList.of(ProgramRunStatus.STARTING, ProgramRunStatus.SUSPENDED,
                                           ProgramRunStatus.STARTING));
    assertPersistedStatus(metadataStoreDataset, txnl, ProgramRunStatus.RUNNING,
                          ImmutableList.of(ProgramRunStatus.STARTING, ProgramRunStatus.RUNNING,
                                           ProgramRunStatus.STARTING));
  }

  private void assertPersistedStatus(final AppMetadataStore metadataStoreDataset, TransactionExecutor txnl,
                                     final long startSourceId, final long runningSourceId,
                                     final long killedSourceId, ProgramRunStatus expectedRunStatus) throws Exception {
    // Add some run records
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    // use sourceId's to ensure runId is unique for each run
    final RunId runId = RunIds.generate(10000 + startSourceId + runningSourceId + killedSourceId);
    final AtomicReference<RunRecordMeta> resultRecord = new AtomicReference<>();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        metadataStoreDataset.recordProgramStart(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                                null, ImmutableMap.<String, String>of(),
                                                ImmutableMap.<String, String>of(),
                                                AppFabricTestHelper.createSourceId(startSourceId));
        metadataStoreDataset.recordProgramRunning(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                                  null, AppFabricTestHelper.createSourceId(runningSourceId));
        metadataStoreDataset.recordProgramStop(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.KILLED,
                                               null, AppFabricTestHelper.createSourceId(killedSourceId));
        resultRecord.set(metadataStoreDataset.getRun(program, runId.getId()));
      }
    });
    Assert.assertEquals(expectedRunStatus, resultRecord.get().getStatus());
  }

  private void assertPersistedStatus(final AppMetadataStore metadataStoreDataset, TransactionExecutor txnl,
                                     @Nullable ProgramRunStatus expectedRunStatus, List<ProgramRunStatus> recordStatuses)
    throws Exception {
    // Add some run records
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId = RunIds.generate(10000);
    final AtomicLong sourceId = new AtomicLong();
    final Map<ProgramRunStatus, Runnable> statusRecordMap =
      getStatusRecordMap(metadataStoreDataset, program, runId, sourceId);
    // Get the record status runnable for each program run status
    final List<Runnable> recordRunnables = Lists.transform(recordStatuses, new Function<ProgramRunStatus, Runnable>() {
      @Nullable
      @Override
      public Runnable apply(ProgramRunStatus runStatus) {
        return statusRecordMap.get(runStatus);
      }
    });
    // Execute all record status runnable and assert the result
    final AtomicReference<RunRecordMeta> resultRecord = new AtomicReference<>();
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Runnable runnable : recordRunnables) {
          if (runnable != null) {
            runnable.run();
          }
        }
        resultRecord.set(metadataStoreDataset.getRun(program, runId.getId()));
      }
    });
    // If expectedRunStatus is null, no run record is expected to be persisted
    if (expectedRunStatus == null) {
      Assert.assertNull(resultRecord.get());
      return;
    }
    Assert.assertNotNull(resultRecord.get());
    // Only one record with the expected status should exist
    Assert.assertEquals(expectedRunStatus, resultRecord.get().getStatus());
  }

  private Map<ProgramRunStatus, Runnable> getStatusRecordMap(final AppMetadataStore metadataStoreDataset,
                                                             final ProgramId program, final RunId runId,
                                                             final AtomicLong sourceId) {
    ImmutableMap.Builder<ProgramRunStatus, Runnable> statusRecordMap = new ImmutableMap.Builder<>();
    statusRecordMap.put(ProgramRunStatus.STARTING, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramStart(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                                null, ImmutableMap.<String, String>of(),
                                                ImmutableMap.<String, String>of(),
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.RUNNING, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramRunning(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                                  null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.SUSPENDED, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramSuspend(program, runId.getId(),
                                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.RESUMING, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramResumed(program, runId.getId(),
                                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.COMPLETED, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramStop(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.COMPLETED, null,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.FAILED, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramStop(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.FAILED, null,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    statusRecordMap.put(ProgramRunStatus.KILLED, new Runnable() {
      @Override
      public void run() {
        metadataStoreDataset.recordProgramStop(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.KILLED, null,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      }
    });
    return statusRecordMap.build();
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
      final RunId runId = RunIds.generate((i + 1) * 10000);
      expected.add(RunIds.getTime(runId, TimeUnit.MILLISECONDS));
      // Start the program and stop it
      final int j = i;
      // A sourceId to keep incrementing for each call of app meta data store persisting
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          metadataStoreDataset.recordProgramStart(
            program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS), null, ImmutableMap.<String, String>of(),
            ImmutableMap.<String, String>of(), AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          metadataStoreDataset.recordProgramRunning(
            program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS) + 1, null,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          metadataStoreDataset.recordProgramStop(
            program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
            STOP_STATUSES.get(j % STOP_STATUSES.size()),
            null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        }
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
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
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
        Iterables.addAll(actual, Iterables.transform(runIds, new Function<RunId, Long>() {
          @Override
          public Long apply(RunId input) {
            return RunIds.getTime(input, TimeUnit.MILLISECONDS);
          }
        }));

        Assert.assertEquals(expected, actual);
        int numBatches = Iterables.size(batches);
        // Each batch needs 2 extra calls to Ticker.read, once during init and once for final condition check
        // Hence the number of batches should be --
        // (num calls to Ticker.read - (2 * numBatches)) / number of elements per batch
        Assert.assertEquals((countingTicker.getNumProcessed() - (2 * numBatches)) / maxScanTimeMillis, numBatches);
      }
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
      final RunId runId = RunIds.generate((i + 1) * 10000);
      expected.add(runId.toString());
      final int index = i;

      // Add every other runId
      if ((i % 2) == 0) {
        expectedHalf.add(runId.toString());
      }

      ProgramRunId programRunId = new ProgramRunId(program.getNamespace(), program.getApplication(),
                                                   program.getType(), program.getProgram(), runId.toString());
      programRunIdSet.add(programRunId);

      //Add every other programRunId
      if ((i % 2) == 0) {
        programRunIdSetHalf.add(programRunId);
      }

      // A sourceId to keep incrementing for each call of app meta data store persisting
      txnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {

          // Start the program and stop it
          metadataStoreDataset.recordProgramStart(
            program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS), null, null, null,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          metadataStoreDataset.recordProgramRunning(
            program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS), null,
            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          metadataStoreDataset.recordProgramStop(program, runId.getId(), RunIds.getTime(runId, TimeUnit.SECONDS),
                                                 ProgramRunStatus.values()[index % ProgramRunStatus.values().length],
                                                 null,
                                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        }
      });
    }

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {

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
      }
    });
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
