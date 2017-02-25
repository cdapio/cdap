/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.UnitTestManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@Category(XSlowTests.class)
public class PartitionRollbackTestRun extends TestFrameworkTestBase {

  private static final String PFS = AppWritingToPartitioned.PFS;
  private static final String OTHER = AppWritingToPartitioned.OTHER;
  private static final String BOTH = PFS + "," + OTHER;
  private static final String PFS_OUT = PFS + ".output.partition";
  private static final String OTHER_OUT = OTHER + ".output.partition";

  private static final String MAPREDUCE = AppWritingToPartitioned.MAPREDUCE;

  private static final PartitionKey KEY_0 = PartitionKey.builder().addField("number", 0).build();
  private static final PartitionKey KEY_1 = PartitionKey.builder().addField("number", 1).build();
  private static final PartitionKey KEY_2 = PartitionKey.builder().addField("number", 2).build();
  private static final PartitionKey KEY_3 = PartitionKey.builder().addField("number", 3).build();
  private static final PartitionKey KEY_4 = PartitionKey.builder().addField("number", 4).build();
  private static final PartitionKey KEY_5 = PartitionKey.builder().addField("number", 5).build();

  class Validator {
    private final UnitTestManager.UnitTestDatasetManager<PartitionedFileSet> pfsManager;
    private final String tableName;
    private final Location location1, location2, location3;
    private final String path3;

    /*
     * Set up some partitions in a partitioned file set.
     * Every time validate() is called it verifies the state is still the same
     *
     * KEY_1: partition with data in the standard location
     * KEY_2: partition with data in a non-standard location
     * KEY_3: data in standard location, but no partition
     * KEY_4: partition is only in Hive, with standard location for KEY_3
     *
     * This sets us up to predictably produce failure in MR:
     * - output to KEY_1 or KEY_3 will fail because the location already exists
     * - output to KEY_2 will fail because the partition already exists
     * - output to KEY_4 will fail because the Hive partition exists
     * - output to KEY_0 or KEY_5 will succeed and we can validate they get rolled back after failure
     *
     * We will set up two datasets in this way, and can then inject failure into either one by configuring its
     * output partition.
     */
    Validator(String datasetName) throws Exception {
      this.tableName = datasetName;

      DataSetManager dsManager = getDataset(datasetName);
      Assert.assertTrue(dsManager instanceof UnitTestManager.UnitTestDatasetManager);
      // TODO (CDAP-3792): remove this hack when TestBase has better support for transactions
      //noinspection unchecked
      pfsManager = (UnitTestManager.UnitTestDatasetManager<PartitionedFileSet>) dsManager;
      final PartitionedFileSet pfs = pfsManager.get();

      // create a partition n=1 in standard path
      final PartitionOutput output1 =  pfs.getPartitionOutput(KEY_1);
      location1 = output1.getLocation();
      try (Writer writer = new OutputStreamWriter(location1.append("file").getOutputStream())) {
        writer.write("1,1\n");
      }
      output1.addPartition();

      // crete a partition n=2 in different path
      final String path2 = "path2";
      location2 = pfs.getEmbeddedFileSet().getLocation(path2);
      try (Writer writer = new OutputStreamWriter(location2.append("file").getOutputStream())) {
        writer.write("2,2\n");
      }
      pfs.addPartition(KEY_2, path2);

      // create some file in the standard location for n=3 and add it to Hive but not the PFS for n=4
      final PartitionOutput output3 =  pfs.getPartitionOutput(KEY_3);
      location3 = output3.getLocation();
      String basePath = pfs.getEmbeddedFileSet().getBaseLocation().toURI().getPath();
      String absPath3 = location3.toURI().getPath();
      Assert.assertTrue(absPath3.startsWith(basePath));
      path3 = absPath3.substring(basePath.length());
      try (Writer writer = new OutputStreamWriter(location3.append("file").getOutputStream())) {
        writer.write("3,3\n");
      }
      ((PartitionedFileSetDataset) pfs).addPartitionToExplore(KEY_4, path3);
      pfsManager.flush();
      validate();
    }

    // TODO (CDAP-3792): remove this hack when TestBase has better support for transactions
    UnitTestManager.UnitTestDatasetManager<PartitionedFileSet> getPfsManager() {
      return pfsManager;
    }

    String getRelativePath3() {
      return path3;
    }

    private void validate() throws Exception {
      final PartitionedFileSet pfs = pfsManager.get();
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          try {
            PartitionDetail detail1 = pfs.getPartition(KEY_1);
            Assert.assertNotNull(detail1);
            Assert.assertEquals(location1, detail1.getLocation());
            Assert.assertTrue(location1.exists());
            Assert.assertTrue(location1.append("file").exists());
            PartitionDetail detail2 = pfs.getPartition(KEY_2);
            Assert.assertNotNull(detail2);
            Assert.assertEquals(location2, detail2.getLocation());
            Assert.assertTrue(location2.exists());
            Assert.assertTrue(location2.append("file").exists());
            PartitionDetail detail3 = pfs.getPartition(KEY_4);
            Assert.assertNull(detail3);
            Assert.assertTrue(location3.exists());
            Assert.assertTrue(location3.append("file").exists());
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });

      // verify that the partitions were added to Hive
      String[] expectedPartitions = { "number=1", "number=2", "number=4" };
      SortedMap<String, Integer> expectedRows = ImmutableSortedMap.of("1", 1, "2", 2, "3", 4);
      try (Connection connection = getQueryClient()) {
        try (ResultSet results = connection.prepareStatement("show partitions " + tableName).executeQuery()) {
          for (String expected : expectedPartitions) {
            Assert.assertTrue(results.next());
            Assert.assertEquals(expected, results.getString(1));
          }
        }
        try (ResultSet results = connection
          .prepareStatement("select * from " + tableName + " order by key").executeQuery()) {
          for (Map.Entry<String, Integer> expected : expectedRows.entrySet()) {
            Assert.assertTrue(results.next());
            Assert.assertEquals(expected.getKey(), results.getString(1));
            Assert.assertEquals(expected.getKey(), results.getString(2));
            Assert.assertEquals(expected.getValue().intValue(), results.getInt(3));
          }
        }
      }
    }
  }

  /*
   * This tests all the following cases:
   *
   *  1. addPartition(location) fails because partition already exists
   *  2. addPartition(location) fails because Hive partition already exists
   *  3. addPartition(location) succeeds but transaction fails
   *  4. partitionOutput.addPartition() fails because partition already exists
   *  5. partitionOutput.addPartition() fails because Hive partition already exists
   *  6. partitionOutput.addPartition() succeeds but transaction fails
   *  7. mapreduce writing partition fails because location already exists
   *  8. mapreduce writing partition fails because partition already exists
   *  9. mapreduce writing partition fails because Hive partition already exists
   *  10. mapreduce writing dynamic partition fails because location already exists
   *  11. mapreduce writing dynamic partition fails because partition already exists
   *  12. mapreduce writing dynamic partition fails because Hive partition already exists
   *  13. multi-output mapreduce writing partition fails because location already exists
   *  13a. first output fails, other output must rollback 0 and 5
   *  13b. second output fails, first output must rollback 0 and 5
   *  14. multi-output mapreduce writing partition fails because partition already exists
   *  14a. first output fails, other output must rollback partition 5
   *  14b. second output fails, first output must rollback partition 5
   *  15. multi-output mapreduce writing partition fails because Hive partition already exists
   *  15a. first output fails, other output must rollback partitions 0 and 5
   *  15b. second output fails, first output must rollback partitions 0 and 5
   *
   * For all these cases, we validate that existing files and partitions are preserved, and newly
   * added files and partitions are rolled back.
   */
  @Test
  public void testPFSRollback() throws Exception {
    ApplicationManager appManager = deployApplication(AppWritingToPartitioned.class);
    MapReduceManager mrManager = appManager.getMapReduceManager(MAPREDUCE);
    int numRuns = 0;

    Validator pfsValidator = new Validator(PFS);
    Validator otherValidator = new Validator(OTHER);
    final UnitTestManager.UnitTestDatasetManager<PartitionedFileSet> pfsManager = pfsValidator.getPfsManager();
    final PartitionedFileSet pfs = pfsManager.get();
    final PartitionedFileSet other = otherValidator.getPfsManager().get();
    final String path3 = pfsValidator.getRelativePath3();

    // 1. addPartition(location) fails because partition already exists
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          pfs.addPartition(KEY_1, path3);
        }
      });
      Assert.fail("Expected tx to fail because partition for number=1 already exists");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();

    // 2. addPartition(location) fails because Hive partition already exists
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          pfs.addPartition(KEY_4, path3);
        }
      });
      Assert.fail("Expected tx to fail because hive partition for number=1 already exists");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();

    // 3. addPartition(location) succeeds but transaction fails
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          pfs.addPartition(KEY_3, path3);
          throw new RuntimeException("fail the tx");
        }
      });
      Assert.fail("Expected tx to fail because it threw a runtime exception");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();

    // 4. partitionOutput.addPartition() fails because partition already exists
    final PartitionOutput output2x =  pfs.getPartitionOutput(KEY_2);
    final Location location2x = output2x.getLocation();
    try (Writer writer = new OutputStreamWriter(location2x.append("file").getOutputStream())) {
      writer.write("2x,2x\n");
    }
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          output2x.addPartition();
        }
      });
      Assert.fail("Expected tx to fail because partition for number=2 already exists");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();
    Assert.assertFalse(location2x.exists());

    // 5. partitionOutput.addPartition() fails because Hive partition already exists
    final PartitionOutput output4x =  pfs.getPartitionOutput(KEY_4);
    final Location location4x = output4x.getLocation();
    try (Writer writer = new OutputStreamWriter(location4x.append("file").getOutputStream())) {
      writer.write("4x,4x\n");
    }
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          output4x.addPartition();
        }
      });
      Assert.fail("Expected tx to fail because hive partition for number=4 already exists");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();
    Assert.assertFalse(location4x.exists());

    // 6. partitionOutput.addPartition() succeeds but transaction fails
    final PartitionOutput output5x =  pfs.getPartitionOutput(KEY_5);
    final Location location5x = output5x.getLocation();
    try (Writer writer = new OutputStreamWriter(location5x.append("file").getOutputStream())) {
      writer.write("5x,5x\n");
    }
    try {
      pfsManager.execute(new Runnable() {
        @Override
        public void run() {
          output5x.addPartition();
          throw new RuntimeException("fail the tx");
        }
      });
      Assert.fail("Expected tx to fail because it threw a runtime exception");
    } catch (TransactionFailureException e) {
      // expected
    }
    pfsValidator.validate();
    Assert.assertFalse(location5x.exists());

    // 7. mapreduce writing partition fails because location already exists
    mrManager.start(ImmutableMap.of(PFS_OUT, "1", "input.text", "1x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();

    // 8. mapreduce writing partition fails because partition already exists
    mrManager.start(ImmutableMap.of(PFS_OUT, "2", "input.text", "2x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_2).getLocation().exists());

    // 9. mapreduce writing partition fails because Hive partition already exists
    mrManager.start(ImmutableMap.of(PFS_OUT, "4", "input.text", "4x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_4).getLocation().exists());

    // 10. mapreduce writing dynamic partition fails because location already exists
    mrManager.start(ImmutableMap.of("input.text", "3x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());

    // 11. mapreduce writing dynamic partition fails because partition already exists
    mrManager.start(ImmutableMap.of("input.text", "2x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_2).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());

    // 12. mapreduce writing dynamic partition fails because Hive partition already exists
    mrManager.start(ImmutableMap.of("input.text", "0x 4x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_0).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_4).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());

    // 13. multi-output mapreduce writing partition fails because location already exists
    // 13a. first output fails, other output must rollback 0 and 5
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, PFS_OUT, "1", "input.text", "0x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(other.getPartitionOutput(KEY_0).getLocation().exists());
    Assert.assertFalse(other.getPartitionOutput(KEY_5).getLocation().exists());
    // 13b. second output fails, first output must rollback 0 and 5
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, OTHER_OUT, "1", "input.text", "0x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_0).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());

    // 14. multi-output mapreduce writing partition fails because partition already exists
    // 14a. first output fails, other output must rollback partition 5
    // TODO: bring this back when CDAP-8766 is fixed
    /*
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, PFS_OUT, "2", OTHER_OUT, "5", "input.text", "2x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(other.getPartitionOutput(KEY_5).getLocation().exists());
    */
    // 14b. second output fails, first output must rollback partition 5
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, PFS_OUT, "5", OTHER_OUT, "2", "input.text", "2x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());

    // 15. multi-output mapreduce writing partition fails because Hive partition already exists
    // 15a. first output fails, other output must rollback partitions 0 and 5
    // TODO: bring this back when CDAP-8766 is fixed
    /*
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, PFS_OUT, "4", "input.text", "0x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(pfs.getPartitionOutput(KEY_4).getLocation().exists());
    Assert.assertFalse(other.getPartitionOutput(KEY_0).getLocation().exists());
    Assert.assertFalse(other.getPartitionOutput(KEY_5).getLocation().exists());
    // 15b. second output fails, first output must rollback partitions 0 and 5
    mrManager.start(ImmutableMap.of("output.datasets", BOTH, OTHER_OUT, "4", "input.text", "0x 5x"));
    mrManager.waitForRuns(ProgramRunStatus.FAILED, ++numRuns, 2, TimeUnit.MINUTES);
    pfsValidator.validate();
    otherValidator.validate();
    Assert.assertFalse(other.getPartitionOutput(KEY_4).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_0).getLocation().exists());
    Assert.assertFalse(pfs.getPartitionOutput(KEY_5).getLocation().exists());
    */
  }
}
