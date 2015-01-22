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

package co.cask.cdap.data2.dataset2.lib;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TimePartitionedFileSetTest extends AbstractDatasetTest {

  static TimePartitionedFileSet fileSet;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("timePartitionedFileSet", "tpfs", FileSetProperties.builder()
      .setBasePath("testDir").build());
    fileSet = getInstance("tpfs");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance("tpfs");
  }

  @Test
  public void testAddGetPartitions() throws IOException, ParseException {
    // this is an arbitrary data to use as the test time
    long time = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("12/10/14 5:10 am").getTime();

    // make sure the file set has no partitions initially
    Assert.assertTrue("should return no partitions", fileSet.getPartitions(0L, Long.MAX_VALUE).isEmpty());
    Assert.assertNull("should return null", fileSet.getPartition(time));

    // add a partition, verify getPartition() works
    fileSet.addPartition(time, "first/partition");
    Assert.assertEquals("first/partition", fileSet.getPartition(time));
    Assert.assertTrue("should return no partitions", fileSet.getPartitions(time + 1, Long.MAX_VALUE).isEmpty());
    Assert.assertTrue("should return no partitions", fileSet.getPartitions(0L, time).isEmpty());

    // verify getPartitions() works with various ranges
    Collection<String> paths = fileSet.getPartitions(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitions(0L, time + 1);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitions(time, time + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());
    paths = fileSet.getPartitions(time - TimeUnit.HOURS.toMillis(1), time + TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertEquals("first/partition", paths.iterator().next());

    // add and verify another partition
    long time1 = time + TimeUnit.HOURS.toMillis(1);
    fileSet.addPartition(time1, "second/partition");
    Assert.assertEquals("second/partition", fileSet.getPartition(time1));

    paths = fileSet.getPartitions(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return two partitions", 2, paths.size());
    Assert.assertTrue(paths.contains("first/partition"));
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitions(time, time + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("first/partition"));
    paths = fileSet.getPartitions(time + TimeUnit.MINUTES.toMillis(30), time1);
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitions(time + TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitions(time - TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
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

    paths = fileSet.getPartitions(0L, Long.MAX_VALUE);
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitions(time, time + TimeUnit.MINUTES.toMillis(30));
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitions(time + TimeUnit.MINUTES.toMillis(30), time1);
    Assert.assertTrue(paths.isEmpty());
    paths = fileSet.getPartitions(time + TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));
    paths = fileSet.getPartitions(time - TimeUnit.MINUTES.toMillis(30), time1 + TimeUnit.MINUTES.toMillis(30));
    Assert.assertEquals("should return one partition", 1, paths.size());
    Assert.assertTrue(paths.contains("second/partition"));

    // try to delete  another partition with the same key
    try {
      fileSet.dropPartition(time);
    } catch (DataSetException e) {
      Assert.fail("Should not have have thrown Exception for removing non-existent partition");
    }
  }

  @Test
  public void testTimePartitionPath() throws Exception {
    Date date = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT).parse("1/1/15 8:42 pm");
    Map<String, String> args = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(args, date.getTime());
    TimePartitionedFileSet ds = getInstance("tpfs", args);

    String outputPath = ds.getUnderlyingFileSet().getOutputLocation().toURI().getPath();
    Assert.assertTrue(outputPath.endsWith("2015-01-01/20-42." + date.getTime()));
  }
}
