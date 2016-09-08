/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Function;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Test AppMetadataStore.
 */
public class AppMetadataStoreTest {
  private static DatasetFramework datasetFramework;
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(Id.Namespace.DEFAULT);
    datasetFramework = injector.getInstance(DatasetFramework.class);
    cConf = injector.getInstance(CConfiguration.class);
  }

  @Test
  public void testScanRunningInRangeWithBatch() throws Exception {
    Id.DatasetInstance storeTable = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "testScanRunningInRange");
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.<String, String>of(), null);
    Assert.assertNotNull(table);
    AppMetadataStore metadataStoreDataset = new AppMetadataStore(table, cConf);

    // Add some run records
    TreeSet<Long> expected = new TreeSet<>();
    for (int i = 0; i < 100; ++i) {
      Id.Application application = Id.Application.from(Id.Namespace.DEFAULT, "app" + i);
      Id.Program program = Id.Program.from(application, ProgramType.values()[i % ProgramType.values().length],
                                           "program" + i);
      RunId runId = RunIds.generate((i + 1) * 10000);
      expected.add(RunIds.getTime(runId, TimeUnit.MILLISECONDS));
      // Start the program and stop it
      metadataStoreDataset.recordProgramStart(program.toEntityId(), runId.getId(),
                                              RunIds.getTime(runId, TimeUnit.SECONDS),
                                              null, null, null);
      metadataStoreDataset.recordProgramStop(program.toEntityId(), runId.getId(),
                                             RunIds.getTime(runId, TimeUnit.SECONDS),
                                             ProgramRunStatus.values()[i % ProgramRunStatus.values().length], null);
    }

    // Run full scan
    runScan(metadataStoreDataset, expected, 0, Long.MAX_VALUE);

    // In all below assertions, TreeSet and metadataStore both have start time inclusive and end time exclusive.
    // Run the scan with time limit
    runScan(metadataStoreDataset, expected.subSet(30 * 10000L, 90 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(90 * 10000));

    runScan(metadataStoreDataset, expected.subSet(90 * 10000L, 101 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(90 * 10000), TimeUnit.MILLISECONDS.toSeconds(101 * 10000));

    // After range
    runScan(metadataStoreDataset, expected.subSet(101 * 10000L, 200 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(101 * 10000), TimeUnit.MILLISECONDS.toSeconds(200 * 10000));

    // Identical start and end time
    runScan(metadataStoreDataset, expected.subSet(31 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(31 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // One unit difference between start and end time
    runScan(metadataStoreDataset, expected.subSet(30 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // Before range
    runScan(metadataStoreDataset, expected.subSet(1000L, 10000L),
            TimeUnit.MILLISECONDS.toSeconds(1000), TimeUnit.MILLISECONDS.toSeconds(10000));
  }

  private void runScan(AppMetadataStore metadataStoreDataset, Set<Long> expected, long startTime, long stopTime) {
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

  private static class CountingTicker extends Ticker {
    private final long elementsPerMillis;
    private int numProcessed = 0;

    public CountingTicker(long elementsPerMillis) {
      this.elementsPerMillis = elementsPerMillis;
    }

    public int getNumProcessed() {
      return numProcessed;
    }

    @Override
    public long read() {
      ++numProcessed;
      return TimeUnit.MILLISECONDS.toNanos(numProcessed / elementsPerMillis);
    }
  }
}
