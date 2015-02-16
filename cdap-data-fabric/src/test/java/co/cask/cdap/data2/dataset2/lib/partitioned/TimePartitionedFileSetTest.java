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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TimePartitionedFileSetTest extends AbstractDatasetTest {

  static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

  @Before
  public void before() throws Exception {
    createInstance("timePartitionedFileSet", "tpfs", FileSetProperties.builder()
      .setBasePath("testDir").build());
  }

  @After
  public void after() throws Exception {
    deleteInstance("tpfs");
  }

  @Test
  public void testAddGetPartitions() throws IOException, ParseException, DatasetManagementException {
    TimePartitionedFileSet fileSet = getInstance("tpfs");

    // this is an arbitrary data to use as the test time
    long time = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("12/10/14 5:10 am").getTime();

    // make sure the file set has no partitions initially
    Assert.assertTrue("should return no partitions", fileSet.getPartitionPaths(0L, Long.MAX_VALUE).isEmpty());
    Assert.assertNull("should return null", fileSet.getPartition(time));

    // add a partition, verify getPartition() works
    fileSet.addPartition(time, "first/partition");
    Assert.assertEquals("first/partition", fileSet.getPartition(time));
    Assert.assertTrue("should return no partitions", fileSet.getPartitionPaths(time + 1, Long.MAX_VALUE).isEmpty());
    Assert.assertTrue("should return no partitions", fileSet.getPartitionPaths(0L, time).isEmpty());

    // verify getPartitionPaths() works with various ranges
    Collection<String> paths = fileSet.getPartitionPaths(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitionPaths(0L, time + 1);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitionPaths(time, time + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitionPaths(time - TimeUnit.HOURS.toMillis(1), time + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());

    // add and verify another partition
    long time1 = time + TimeUnit.HOURS.toMillis(1);
    fileSet.addPartition(time1, "second/partition");
    Assert.assertEquals("second/partition", fileSet.getPartition(time1));

    paths = fileSet.getPartitionPaths(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return two partitions", 2, paths.size());
    Assert.assertTrue(paths.contains("first/partition"));
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitionPaths(time, time + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("first/partition"));
    paths = fileSet.getPartitionPaths(time + TimeUnit.MINUTES.toMillis(30), time1);
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitionPaths(time + TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitionPaths(time - TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return two partition", 2, paths.size());
    Assert.assertTrue(paths.contains("first/partition"));
    Assert.assertTrue(paths.contains("second/partition"));

    // try to add another partition with the same key
    try {
      fileSet.addPartition(time1, "third/partition");
      Assert.fail("Should have thrown Exception for duplicate partition");
    } catch (DataSetException e) {
      //expected
    }

    // remove first partition and validate
    fileSet.dropPartition(time);
    Assert.assertNull("should return null", fileSet.getPartition(time));

    paths = fileSet.getPartitionPaths(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitionPaths(time, time + TimeUnit.MINUTES.toMillis(30));
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitionPaths(time + TimeUnit.MINUTES.toMillis(30), time1);
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitionPaths(time + TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitionPaths(time - TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));

    // try to delete  another partition with the same key
    try {
      fileSet.dropPartition(time);
    } catch (DataSetException e) {
      Assert.fail("Should not have have thrown Exception for removing non-existent partition");
    }
  }

  /**
   * Tests that the output file path is set correctly, based on the output partition time.
   */
  @Test
  public void testOutputPartitionPath() throws Exception {
    Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("1/1/15 8:42 pm");
    Map<String, String> args = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(args, date.getTime());
    TimePartitionedFileSet ds = getInstance("tpfs", args);

    String outputPath = ds.getUnderlyingFileSet().getOutputLocation().toURI().getPath();
    Assert.assertTrue(outputPath.endsWith("2015-01-01/20-42." + date.getTime()));

    Map<String, String> outputConfig = ds.getOutputFormatConfiguration();
    Assert.assertTrue(outputConfig.get(FileOutputFormat.OUTDIR).endsWith("2015-01-01/20-42." + date.getTime()));
  }

  /**
   * Tests that the TPFS sets the file input paths correctly for the input time range.
   */
  @Test
  public void testInputPartitionPaths() throws Exception {
    // make sure the dataset has no partitions
    final TimePartitionedFileSet tpfs = getInstance("tpfs");
    Assert.assertTrue(tpfs.getPartitionPaths(0L, Long.MAX_VALUE).isEmpty());

    Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("6/4/12 10:00 am");
    final long time = date.getTime();
    newTransactionExecutor((TransactionAware) tpfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        tpfs.addPartition(time, "file");
        tpfs.addPartition(time + 5 * MINUTE, "file5");
        tpfs.addPartition(time + 10 * MINUTE, "file10");
        tpfs.addPartition(time + 12 * MINUTE, "file12");
      }
    });

    validateInputPaths(time, -10, -5);
    validateInputPaths(time, -10, 2, "file");
    validateInputPaths(time, 1, 11, "file5", "file10");
    validateInputPaths(time, 1, 15, "file5", "file10", "file12");
    validateInputPaths(time, 5, 10, "file5");
  }

  /**
   * Validates that the output configuration of the tpfs, when instantiated with (time - start * minutes) as
   * input start time and (time + end * minutes) as input end time, returns the expected list of paths.
   */
  private void validateInputPaths(long time, long start, long end, final String ... expected)
    throws IOException, DatasetManagementException, InterruptedException, TransactionFailureException {
    Map<String, String> arguments = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(arguments, time + start * MINUTE);
    TimePartitionedFileSetArguments.setInputEndTime(arguments, time + end * MINUTE);
    final TimePartitionedFileSet tpfs = getInstance("tpfs", arguments);
    newTransactionExecutor((TransactionAware) tpfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Map<String, String> inputConfig = tpfs.getInputFormatConfiguration();
        String inputs = inputConfig.get("mapred.input.dir");
        Assert.assertNotNull(inputs);
        if (expected.length == 0) {
          Assert.assertTrue(inputs.isEmpty());
          return;
        }
        String[] inputPaths = inputs.split(",");
        Assert.assertEquals(expected.length, inputPaths.length);
        // order is not guaranteed.
        Arrays.sort(expected);
        Arrays.sort(inputPaths);
        for (int i = 0; i < expected.length; i++) {
          // every input path is absolute, whereas expected paths are relative
          Assert.assertTrue("path #" + i + " does not match", inputPaths[i].endsWith(expected[i]));
        }
      }
    });
  }
}
