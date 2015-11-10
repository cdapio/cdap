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
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimePartitionedFileSetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
  static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
  static final long HOUR = TimeUnit.HOURS.toMillis(1);
  static final long MAX = Long.MAX_VALUE;

  private static final Id.DatasetInstance TPFS_INSTANCE =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "tpfs");

  @Before
  public void before() throws Exception {
    dsFrameworkUtil.createInstance("timePartitionedFileSet", TPFS_INSTANCE, FileSetProperties.builder()
      .setBasePath("testDir").build());
  }

  @After
  public void after() throws Exception {
    dsFrameworkUtil.deleteInstance(TPFS_INSTANCE);
  }

  @Test
  public void testPartitionMetadata() throws Exception {
    final TimePartitionedFileSet tpfs = dsFrameworkUtil.getInstance(TPFS_INSTANCE);
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) tpfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // make sure the dataset has no partitions
        validateTimePartitions(tpfs, 0L, MAX, Collections.<Long, String>emptyMap());

        Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("6/4/12 10:00 am");
        long time = date.getTime();

        // keep track of all the metadata added
        Map<String, String> allMetadata = Maps.newHashMap();

        Map<String, String> metadata = ImmutableMap.of("key1", "value1",
                                                       "key2", "value3",
                                                       "key100", "value4");
        tpfs.addPartition(time, "file", metadata);
        allMetadata.putAll(metadata);
        TimePartitionDetail partitionByTime = tpfs.getPartitionByTime(time);
        Assert.assertNotNull(partitionByTime);
        Assert.assertEquals(metadata, partitionByTime.getMetadata().asMap());

        tpfs.addMetadata(time, "key3", "value4");
        allMetadata.put("key3", "value4");

        try {
          // attempting to update an existing key throws a DatasetException
          tpfs.addMetadata(time, "key3", "value5");
          Assert.fail("Expected not to be able to update an existing metadata entry");
        } catch (DataSetException expected) {
        }

        Map<String, String> newMetadata = ImmutableMap.of("key4", "value4",
                                                          "key5", "value5");
        tpfs.addMetadata(time, newMetadata);
        allMetadata.putAll(newMetadata);

        partitionByTime = tpfs.getPartitionByTime(time);
        Assert.assertNotNull(partitionByTime);
        Assert.assertEquals(allMetadata, partitionByTime.getMetadata().asMap());
      }
    });
  }

  @Test
  public void testAddGetPartitions() throws Exception {
    final TimePartitionedFileSet fileSet = dsFrameworkUtil.getInstance(TPFS_INSTANCE);

    dsFrameworkUtil.newTransactionExecutor((TransactionAware) fileSet).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // this is an arbitrary data to use as the test time
        long time = DATE_FORMAT.parse("12/10/14 5:10 am").getTime();
        long time2 = time + HOUR;
        String firstPath = "first/partition";
        String secondPath = "second/partition";

        // make sure the file set has no partitions initially
        validateTimePartition(fileSet, time, null);
        validateTimePartitions(fileSet, 0L, MAX, Collections.<Long, String>emptyMap());

        // add a partition, verify getPartition() works
        fileSet.addPartition(time, firstPath);
        validateTimePartition(fileSet, time, firstPath);

        Map<Long, String> expectNone = Collections.emptyMap();
        Map<Long, String> expectFirst = ImmutableMap.of(time, firstPath);
        Map<Long, String> expectSecond = ImmutableMap.of(time2, secondPath);
        Map<Long, String> expectBoth = ImmutableMap.of(time, firstPath, time2, secondPath);

        // verify various ways to list partitions with various ranges
        validateTimePartitions(fileSet, time + MINUTE, MAX, expectNone);
        validateTimePartitions(fileSet, 0L, time, expectNone);
        validateTimePartitions(fileSet, 0L, MAX, expectFirst);
        validateTimePartitions(fileSet, 0L, time + MINUTE, expectFirst);
        validateTimePartitions(fileSet, 0L, time + MINUTE, expectFirst);
        validateTimePartitions(fileSet, 0L, time + HOUR, expectFirst);
        validateTimePartitions(fileSet, time - HOUR, time + HOUR, expectFirst);

        // add and verify another partition
        fileSet.addPartition(time2, secondPath);
        validateTimePartition(fileSet, time2, secondPath);

        // verify various ways to list partitions with various ranges
        validateTimePartitions(fileSet, 0L, MAX, expectBoth);
        validateTimePartitions(fileSet, time, time + 30 * MINUTE, expectFirst);
        validateTimePartitions(fileSet, time + 30 * MINUTE, time2, expectNone);
        validateTimePartitions(fileSet, time + 30 * MINUTE, time2 + 30 * MINUTE, expectSecond);
        validateTimePartitions(fileSet, time - 30 * MINUTE, time2 + 30 * MINUTE, expectBoth);

        // try to add another partition with the same key
        try {
          fileSet.addPartition(time2, "third/partition");
          Assert.fail("Should have thrown Exception for duplicate partition");
        } catch (DataSetException e) {
          //expected
        }

        // remove first partition and validate
        fileSet.dropPartition(time);
        validateTimePartition(fileSet, time, null);

        // verify various ways to list partitions with various ranges
        validateTimePartitions(fileSet, 0L, MAX, expectSecond);
        validateTimePartitions(fileSet, time, time + 30 * MINUTE, expectNone);
        validateTimePartitions(fileSet, time + 30 * MINUTE, time2, expectNone);
        validateTimePartitions(fileSet, time + 30 * MINUTE, time2 + 30 * MINUTE, expectSecond);
        validateTimePartitions(fileSet, time - 30 * MINUTE, time2 + 30 * MINUTE, expectSecond);

        // try to delete  another partition with the same key
        try {
          fileSet.dropPartition(time);
        } catch (DataSetException e) {
          Assert.fail("Should not have have thrown Exception for removing non-existent partition");
        }
      }
    });
  }

  /**
   * Tests that the output file path is set correctly, based on the output partition time.
   */
  @Test
  public void testOutputPartitionPath() throws Exception {
    // test specifying output time
    Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("1/1/15 8:42 pm");
    Map<String, String> args = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(args, date.getTime());
    TimeZone timeZone = Calendar.getInstance().getTimeZone();
    TimePartitionedFileSetArguments.setOutputPathFormat(args, "yyyy-MM-dd/HH_mm", timeZone.getID());
    TimePartitionedFileSet ds = dsFrameworkUtil.getInstance(TPFS_INSTANCE, args);

    String outputPath = ds.getEmbeddedFileSet().getOutputLocation().toURI().getPath();
    Assert.assertTrue(outputPath.endsWith("2015-01-01/20_42"));

    Map<String, String> outputConfig = ds.getOutputFormatConfiguration();
    Assert.assertTrue(outputConfig.get(FileOutputFormat.OUTDIR).endsWith("2015-01-01/20_42"));

    // test specifying output time and partition key -> time should prevail
    PartitionKey key = PartitionKey.builder()
      .addIntField("year", 2014)
      .addIntField("month", 1)
      .addIntField("day", 1)
      .addIntField("hour", 20)
      .addIntField("minute", 54)
      .build();
    TimePartitionedFileSet ds1 = dsFrameworkUtil.getInstance(TPFS_INSTANCE, args);
    TimePartitionedFileSetArguments.setOutputPartitionKey(args, key);
    outputConfig = ds1.getOutputFormatConfiguration();
    Assert.assertTrue(outputConfig.get(FileOutputFormat.OUTDIR).endsWith("2015-01-01/20_42"));

    args.clear();
    TimePartitionedFileSetArguments.setOutputPartitionKey(args, key);
    TimePartitionedFileSet ds2 = dsFrameworkUtil.getInstance(TPFS_INSTANCE, args);
    outputConfig = ds2.getOutputFormatConfiguration();
    Assert.assertTrue(outputConfig.get(FileOutputFormat.OUTDIR).endsWith("54"));

    args.clear();
    TimePartitionedFileSet ds3 = dsFrameworkUtil.getInstance(TPFS_INSTANCE, args);
    try {
      ds3.getOutputFormatConfiguration();
      Assert.fail("getOutputFormatConfiguration should have failed with neither output time nor partition key");
    } catch (DataSetException e) {
      // expected
    }
  }

  /**
   * Tests that the TPFS sets the file input paths correctly for the input time range.
   */
  @Test
  public void testInputPartitionPaths() throws Exception {
    // make sure the dataset has no partitions
    final TimePartitionedFileSet tpfs = dsFrameworkUtil.getInstance(TPFS_INSTANCE);
    validateTimePartitions(tpfs, 0L, MAX, Collections.<Long, String>emptyMap());

    Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("6/4/12 10:00 am");
    final long time = date.getTime();
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) tpfs).execute(new TransactionExecutor.Subroutine() {
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
    final TimePartitionedFileSet tpfs = dsFrameworkUtil.getInstance(TPFS_INSTANCE, arguments);
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) tpfs).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Map<String, String> inputConfig = tpfs.getInputFormatConfiguration();
        String inputs = inputConfig.get(FileInputFormat.INPUT_DIR);
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

  @Test
  public void testPartitionsForTimeRange() throws Exception {
    DateFormat format = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
    for (Object[] test : rangeTests) {
      try {
        long start = test[0] instanceof Long ? (Long) test[0] : format.parse((String) test[0]).getTime();
        long stop = test[1] instanceof Long ? (Long) test[1] : format.parse((String) test[1]).getTime();
        List<PartitionFilter> filters = TimePartitionedFileSetDataset.partitionFiltersForTimeRange(start, stop);
        //Assert.assertEquals(test.length - 2, filters.size());
        Set<String> expectedSet = Sets.newHashSet();
        for (int i = 2; i < test.length; i++) {
          expectedSet.add((String) test[i]);
        }
        Set<String> actualSet = Sets.newHashSet();
        for (PartitionFilter filter : filters) {
          actualSet.add(filter == null ? null : filter.toString());
        }
        Assert.assertEquals(expectedSet, actualSet);
      } catch (Throwable t) {
        throw new Exception("Failed for range " + test[0] + "..." + test[1], t);
      }
    }
  }

  private static final Object[][] rangeTests = {
    { 0L, MAX, null }, // no bounds -> no filter

    { "12/10/14 5:10 am", "12/10/14 5:10 am" }, // empty range
    { "12/10/14 5:10 am", "12/10/14 5:09 am" }, // empty range

    { "12/10/14 5:10 am", "12/10/14 5:40 am", "[year==2014, month==12, day==10, hour==5, minute in [10...40]]" },

    { "12/10/14 5:10 am", "12/10/14 6:40 am", "[year==2014, month==12, day==10, hour==5, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==10, hour==6, minute in [null...40]]" },

    { "12/10/14 5:10 am", "12/10/14 7:40 am", "[year==2014, month==12, day==10, hour==5, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==10, hour==6]",
      /* */                                   "[year==2014, month==12, day==10, hour==7, minute in [null...40]]" },

    { "12/10/14 4:10 am", "12/10/14 7:40 am", "[year==2014, month==12, day==10, hour==4, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==10, hour in [5...7]]",
      /* */                                   "[year==2014, month==12, day==10, hour==7, minute in [null...40]]" },

    { "12/09/14 9:10 pm", "12/10/14 7:40 am", "[year==2014, month==12, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==12, day==10, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==10, hour==7, minute in [null...40]]" },

    { "12/09/14 9:10 pm", "12/11/14 7:40 am", "[year==2014, month==12, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==12, day==10]",
      /* */                                   "[year==2014, month==12, day==11, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==11, hour==7, minute in [null...40]]" },

    { "12/09/14 9:10 pm", "12/12/14 7:40 am", "[year==2014, month==12, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==12, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==12, day in [10...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "11/09/14 9:10 pm", "12/12/14 7:40 am", "[year==2014, month==11, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==11, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==11, day in [10...null]]",
      /* */                                   "[year==2014, month==12, day in [null...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "10/09/14 9:10 pm", "12/12/14 7:40 am", "[year==2014, month==10, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==10, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==10, day in [10...null]]",
      /* */                                   "[year==2014, month==11]",
      /* */                                   "[year==2014, month==12, day in [null...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "08/09/14 9:10 pm", "12/12/14 7:40 am", "[year==2014, month==8, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2014, month==8, day==9, hour in [22...null]]",
      /* */                                   "[year==2014, month==8, day in [10...null]]",
      /* */                                   "[year==2014, month in [9...12]]",
      /* */                                   "[year==2014, month==12, day in [null...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "08/09/13 9:10 pm", "12/12/14 7:40 am", "[year==2013, month==8, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2013, month==8, day==9, hour in [22...null]]",
      /* */                                   "[year==2013, month==8, day in [10...null]]",
      /* */                                   "[year==2013, month in [9...null]]",
      /* */                                   "[year==2014, month in [null...12]]",
      /* */                                   "[year==2014, month==12, day in [null...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "08/09/12 9:10 pm", "12/12/14 7:40 am", "[year==2012, month==8, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2012, month==8, day==9, hour in [22...null]]",
      /* */                                   "[year==2012, month==8, day in [10...null]]",
      /* */                                   "[year==2012, month in [9...null]]",
      /* */                                   "[year==2013]",
      /* */                                   "[year==2014, month in [null...12]]",
      /* */                                   "[year==2014, month==12, day in [null...12]]",
      /* */                                   "[year==2014, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2014, month==12, day==12, hour==7, minute in [null...40]]" },

    { "08/09/12 9:10 pm", "12/12/15 7:40 am", "[year==2012, month==8, day==9, hour==21, minute in [10...null]]",
      /* */                                   "[year==2012, month==8, day==9, hour in [22...null]]",
      /* */                                   "[year==2012, month==8, day in [10...null]]",
      /* */                                   "[year==2012, month in [9...null]]",
      /* */                                   "[year in [2013...2015]]",
      /* */                                   "[year==2015, month in [null...12]]",
      /* */                                   "[year==2015, month==12, day in [null...12]]",
      /* */                                   "[year==2015, month==12, day==12, hour in [null...7]]",
      /* */                                   "[year==2015, month==12, day==12, hour==7, minute in [null...40]]" },

    { "08/09/12 9:10 pm", MAX, "[year in [2013...null]]",
      /* */                    "[year==2012, month in [9...null]]",
      /* */                    "[year==2012, month==8, day in [10...null]]",
      /* */                    "[year==2012, month==8, day==9, hour in [22...null]]",
      /* */                    "[year==2012, month==8, day==9, hour==21, minute in [10...null]]" },

    { -1L, "08/09/12 9:10 pm", "[year in [null...2012]]",
      /* */                    "[year==2012, month in [null...8]]",
      /* */                    "[year==2012, month==8, day in [null...9]]",
      /* */                    "[year==2012, month==8, day==9, hour in [null...21]]",
      /* */                    "[year==2012, month==8, day==9, hour==21, minute in [null...10]]" },

    { "12/31/12 11:59 pm", "1/1/13 00:00 am", "[year==2012, month==12, day==31, hour==23, minute in [59...null]]" },

    // code assumes all months can have 31 days. Hence we have an unnecessary filter here that matches nothing
    { "11/30/12 11:59 pm", "12/1/12 00:00 am", "[year==2012, month==11, day in [31...null]]",
      /* */                                    "[year==2012, month==11, day==30, hour==23, minute in [59...null]]" },

    { "10/31/12 11:59 pm", "11/1/12 00:00 am", "[year==2012, month==10, day==31, hour==23, minute in [59...null]]" },

    { "10/21/12 11:59 pm", "10/22/12 00:00 am", "[year==2012, month==10, day==21, hour==23, minute in [59...null]]" },

    { "10/21/12 8:59 am", "10/21/12 09:00 am", "[year==2012, month==10, day==21, hour==8, minute in [59...null]]" },

    { "10/21/12 8:31 am", "10/21/12 08:32 am", "[year==2012, month==10, day==21, hour==8, minute==31]" },

    { "10/21/12 8:00 am", "10/21/12 08:32 am", "[year==2012, month==10, day==21, hour==8, minute in [0...32]]" },

    { "10/21/12 8:00 am", "10/21/12 09:00 am", "[year==2012, month==10, day==21, hour==8, minute in [0...null]]" },

    // the first three really are equivalent to [year==2012, month==10], but the code is not smart enough to know that
    { "10/01/12 00:00 am", "11/01/12 09:00 am", "[year==2012, month==10, day==1, hour==0, minute in [0...null]]",
      /* */                                     "[year==2012, month==10, day==1, hour in [1...null]]",
      /* */                                     "[year==2012, month==10, day in [2...null]]",
      /* */                                     "[year==2012, month==11, day==1, hour in [null...9]]" },
  };

  private void validateTimePartition(TimePartitionedFileSet dataset, long time, String path) {
    PartitionDetail partitionDetail = dataset.getPartitionByTime(time);
    Assert.assertEquals(path == null, partitionDetail == null);
    Assert.assertTrue(path == null || partitionDetail == null || path.equals(partitionDetail.getRelativePath()));
  }

  private void validateTimePartitions(TimePartitionedFileSet dataset,
    long startTime, long endTime, Map<Long, String> expected) {
    // validate getPartitionsByTime()
    Set<TimePartitionDetail> partitions = dataset.getPartitionsByTime(startTime, endTime);
    Assert.assertEquals(expected.size(), partitions.size());
    for (TimePartitionDetail partition : partitions) {
      Assert.assertEquals(expected.get(partition.getTime()), partition.getRelativePath());
    }
  }

  @Test
  public void testTimePartitionedInputArguments() throws Exception {

    final long time8 = DATE_FORMAT.parse("10/17/2014 8:42 am").getTime();
    final long time9 = DATE_FORMAT.parse("10/17/2014 9:42 am").getTime();

    final String path8 = "8:42";
    final String path9 = "9:42";

    final PartitionFilter filter9 = PartitionFilter.builder().addRangeCondition("hour", 9, null).build();

    // add a few partitions
    {
      final TimePartitionedFileSet dataset = dsFrameworkUtil.getInstance(TPFS_INSTANCE);
      dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          dataset.addPartition(time8, path8);
          dataset.addPartition(time9, path9);
        }
      });
    }

    // test specifying time range for input
    Map<String, String> arguments = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(arguments, time8 - 30 * MINUTE);
    TimePartitionedFileSetArguments.setInputEndTime(arguments, time8 + 30 * MINUTE);
    testInputConfiguration(arguments, path8);

    // add a partition filter. it should not have an effect as long as there is a time range
    TimePartitionedFileSetArguments.setInputPartitionFilter(arguments, filter9);
    testInputConfiguration(arguments, path8);

    // test specifying input with a partition filter
    arguments.clear();
    TimePartitionedFileSetArguments.setInputPartitionFilter(arguments, filter9);
    testInputConfiguration(arguments, path9);

    // test specifying only a start time or only an end time for input, or none
    arguments.clear();
    TimePartitionedFileSetArguments.setInputStartTime(arguments, time8 + 30 * MINUTE);
    testInputConfigurationFailure(arguments, " with only a start time");
    arguments.clear();
    TimePartitionedFileSetArguments.setInputEndTime(arguments, time8 + 30 * MINUTE);
    testInputConfigurationFailure(arguments, " with only an end time");
  }

  private void testInputConfiguration(Map<String, String> arguments, final String expectedPath) throws Exception {
    final TimePartitionedFileSet dataset = dsFrameworkUtil.getInstance(TPFS_INSTANCE, arguments);
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Map<String, String> inputConf = dataset.getInputFormatConfiguration();
        String input = inputConf.get(FileInputFormat.INPUT_DIR);
        Assert.assertNotNull(input);
        String[] inputs = input.split(",");
        Assert.assertEquals(1, inputs.length);
        Assert.assertTrue(inputs[0].endsWith(expectedPath));
      }});
  }

  private void testInputConfigurationFailure(Map<String, String> arguments, final String why) throws Exception {
    final TimePartitionedFileSet dataset = dsFrameworkUtil.getInstance(TPFS_INSTANCE, arguments);
    dsFrameworkUtil.newTransactionExecutor((TransactionAware) dataset).execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        try {
          dataset.getInputFormatConfiguration();
          Assert.fail("getInputFormatConfiguration should fail " + why);
        } catch (Exception e) {
          // expected
        }
      }});
  }

}
