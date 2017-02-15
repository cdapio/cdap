/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class PartitionCorrectorTestRun extends TestFrameworkTestBase {

  private static final DateFormat DATE_FORMAT =
    DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US);

  @Test
  public void testPartitionCorrector() throws Exception {

    ApplicationManager appManager = deployApplication(PartitionExploreCorrectorTestApp.class);

    final int numPartitions = 10;
    addDatasetInstance(TimePartitionedFileSet.class.getName(), "tpfs",
                       PartitionedFileSetProperties.builder()
                         .setExploreFormat("csv")
                         .setExploreSchema("key int, value string")
                         .setEnableExploreOnCreate(true)
                         .build());
    DataSetManager<TimePartitionedFileSet> tpfsManager = getDataset("tpfs");
    Date date = DATE_FORMAT.parse("6/4/12 10:00 am");
    long baseTime = date.getTime();
    for (int i = 0; i < numPartitions; i++) {
      createPartition(tpfsManager, baseTime + TimeUnit.MINUTES.toMillis(1) * i, i);
    }
    validateAllPartitions(numPartitions);
    dropAllPartitions();
    validateAllPartitions(0);

    // all partitions are missing. drop/recrete Hive table and add all partitions
    WorkerManager workerManager = appManager.getWorkerManager("PartitionWorker")
      .start(ImmutableMap.of(
        "dataset.name", "tpfs",
        "batch.size", "5",
        "verbose", "true"));
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);
    validateAllPartitions(numPartitions);

    dropAllPartitions();
    for (int i = numPartitions; i < 2 * numPartitions; i++) {
      createPartition(tpfsManager, baseTime + TimeUnit.MINUTES.toMillis(1) * i, i);
    }
    validateAllPartitions(numPartitions);

    // some partitions are missing, some present keep the Hive table and try to add all partitions
    workerManager = appManager.getWorkerManager("PartitionWorker")
      .start(ImmutableMap.of(
        "dataset.name", "tpfs",
        "batch.size", "8",
        "verbose", "false",
        "disable.explore", "false"));
    workerManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 60, TimeUnit.SECONDS);
    validateAllPartitions(2 * numPartitions);
  }

  private void validateAllPartitions(int numPartitions) throws Exception {
    try (Connection connection = getQueryClient()) {
      ResultSet results = connection.prepareStatement("select count(*) as count from dataset_tpfs").executeQuery();
      Assert.assertTrue(results.next());
      Assert.assertEquals(numPartitions, results.getInt("count"));
    }
  }

  private void dropAllPartitions() throws Exception {
    try (Connection connection = getQueryClient()) {
      Assert.assertTrue(connection.prepareStatement("alter table dataset_tpfs drop partition (year=2012)").execute());
    }
  }

  private void createPartition(DataSetManager<TimePartitionedFileSet> tpfsManager, long time, int i) throws Exception {
    TimePartitionedFileSet tpfs = tpfsManager.get();
    TimePartitionOutput output = tpfs.getPartitionOutput(time);
    try (PrintStream out = new PrintStream(output.getLocation().append("file").getOutputStream())) {
      out.println(String.format("%d,x%d", i, i));
    }
    output.addPartition();
    tpfsManager.flush();
  }
}
