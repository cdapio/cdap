/*
 * Copyright © 2014 Cask Data, Inc.
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
package io.cdap.cdap.hbase.wd;

import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.test.XSlowTests;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Provides basic tests for row key distributor
 */
@Category(XSlowTests.class)
public abstract class RowKeyDistributorTestBase {

  // Controls for test suite for whether to run BeforeClass/AfterClass
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  protected static final String TABLE_NAME = "table";
  protected static final byte[] CF = Bytes.toBytes("colfam");
  protected static final byte[] QUAL = Bytes.toBytes("qual");
  private final AbstractRowKeyDistributor keyDistributor;
  private static HBaseTestingUtility testingUtility;
  private static Table table;

  public RowKeyDistributorTestBase(AbstractRowKeyDistributor keyDistributor) {
    this.keyDistributor = keyDistributor;
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (!runBefore) {
      return;
    }

    testingUtility = new HBaseTestingUtility();
    Configuration hConf = testingUtility.getConfiguration();
    hConf.set("yarn.is.minicluster", "true");
    // Tune down the connection thread pool size
    hConf.setInt("hbase.hconnection.threads.core", 5);
    hConf.setInt("hbase.hconnection.threads.max", 10);
    // Tunn down handler threads in regionserver
    hConf.setInt("hbase.regionserver.handler.count", 10);

    // Set to random port
    hConf.setInt("hbase.master.port", Networks.getRandomPort());
    hConf.setInt("hbase.master.info.port", Networks.getRandomPort());
    hConf.setInt("hbase.regionserver.port", Networks.getRandomPort());
    hConf.setInt("hbase.regionserver.info.port", Networks.getRandomPort());

    testingUtility.startMiniCluster();
    table = testingUtility.createTable(TableName.valueOf(TABLE_NAME), CF);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (!runAfter) {
      return;
    }
    testingUtility.shutdownMiniCluster();
  }

  @After
  public void after() throws Exception {
    testingUtility.truncateTable(table.getTableDescriptor().getTableName());
  }

  /** Testing simple get. */
  @Test
  public void testGet() throws IOException, InterruptedException {
    // Testing simple get
    byte[] key = new byte[] {123, 124, 122};
    byte[] distributedKey = keyDistributor.getDistributedKey(key);
    byte[] value = Bytes.toBytes("some");

    table.put(new Put(distributedKey).addColumn(CF, QUAL, value));

    Result result = table.get(new Get(distributedKey));
    Assert.assertArrayEquals(key, keyDistributor.getOriginalKey(result.getRow()));
    Assert.assertArrayEquals(value, result.getValue(CF, QUAL));
  }

  /** Test scan with start and stop key. */
  @Test
  public void testSimpleScanBounded() throws IOException {
    long origKeyPrefix = System.currentTimeMillis();

    int seekIntervalMinValue = 100;
    int seekIntervalMaxValue = 899;
    byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
    byte[] stopKey = Bytes.toBytes(origKeyPrefix + seekIntervalMaxValue + 1);
    Scan scan = new Scan(startKey, stopKey);
    testSimpleScanInternal(origKeyPrefix, scan, 500, 500, seekIntervalMinValue, seekIntervalMaxValue);
  }

  /** Test scan over the whole table. */
  @Test
  public void testSimpleScanUnbounded() throws IOException {
    long origKeyPrefix = System.currentTimeMillis();
    testSimpleScanInternal(origKeyPrefix, new Scan(), 500, 500, 0, 999);
  }

  /** Test scan without stop key. */
  @Test
  public void testSimpleScanWithoutStopKey() throws IOException {
    long origKeyPrefix = System.currentTimeMillis();
    int seekIntervalMinValue = 100;
    byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
    testSimpleScanInternal(origKeyPrefix, new Scan(startKey), 500, 500, 100, 999);
  }

  /** Test scan with start and stop key. */
  @Test
  public void testMapReduceBounded() throws IOException, InterruptedException, ClassNotFoundException {
    long origKeyPrefix = System.currentTimeMillis();

    int seekIntervalMinValue = 100;
    int seekIntervalMaxValue = 899;
    byte[] startKey = Bytes.toBytes(origKeyPrefix + seekIntervalMinValue);
    byte[] stopKey = Bytes.toBytes(origKeyPrefix + seekIntervalMaxValue + 1);
    Scan scan = new Scan(startKey, stopKey);
    testMapReduceInternal(origKeyPrefix, scan, 500, 500, seekIntervalMinValue, seekIntervalMaxValue);
  }

  /** Test scan over the whole table. */
  @Test
  public void testMapReduceUnbounded() throws IOException, InterruptedException, ClassNotFoundException {
    long origKeyPrefix = System.currentTimeMillis();
    testMapReduceInternal(origKeyPrefix, new Scan(), 500, 500, 0, 999);
  }

  private int writeTestData(long origKeyPrefix, int numRows, int rowKeySeed,
                            int seekIntervalMinValue, int seekIntervalMaxValue) throws IOException {
    int valuesCountInSeekInterval = 0;
    for (int i = 0; i < numRows; i++) {
      int val = rowKeySeed + i - i * (i % 2) * 2; // i.e. 500, 499, 502, 497, 504, ...
      valuesCountInSeekInterval += (val >= seekIntervalMinValue && val <= seekIntervalMaxValue) ? 1 : 0;
      byte[] key = Bytes.toBytes(origKeyPrefix + val);
      byte[] distributedKey = keyDistributor.getDistributedKey(key);
      byte[] value = Bytes.toBytes(val);
      table.put(new Put(distributedKey).add(CF, QUAL, value));
    }
    return valuesCountInSeekInterval;
  }

  private void testSimpleScanInternal(long origKeyPrefix, Scan scan, int numValues, int startWithValue,
                                      int seekIntervalMinValue, int seekIntervalMaxValue) throws IOException {
    int valuesCountInSeekInterval =
            writeTestData(origKeyPrefix, numValues, startWithValue, seekIntervalMinValue, seekIntervalMaxValue);

    // TODO: add some filters to the scan for better testing
    ResultScanner distributedScanner = DistributedScanner.create(table, scan, keyDistributor,
                                                                 Executors.newFixedThreadPool(2));

    Result previous = null;
    int countMatched = 0;
    for (Result current : distributedScanner) {
      countMatched++;
      if (previous != null) {
        byte[] currentRowOrigKey = keyDistributor.getOriginalKey(current.getRow());
        byte[] previousRowOrigKey = keyDistributor.getOriginalKey(previous.getRow());
        Assert.assertTrue(Bytes.compareTo(currentRowOrigKey, previousRowOrigKey) >= 0);

        int currentValue = Bytes.toInt(current.getValue(CF, QUAL));
        Assert.assertTrue(currentValue >= seekIntervalMinValue);
        Assert.assertTrue(currentValue <= seekIntervalMaxValue);
      }
      previous = current;
    }

    Assert.assertEquals(valuesCountInSeekInterval, countMatched);
  }

  private void testMapReduceInternal(long origKeyPrefix, Scan scan, int numValues, int startWithValue,
                                     int seekIntervalMinValue, int seekIntervalMaxValue)
          throws IOException, InterruptedException, ClassNotFoundException {
    int valuesCountInSeekInterval =
            writeTestData(origKeyPrefix, numValues, startWithValue, seekIntervalMinValue, seekIntervalMaxValue);

    // Reading data
    Configuration conf = new Configuration(testingUtility.getConfiguration());
    conf.set("fs.defaultFS", "file:///");
    conf.set("fs.default.name", "file:///");
    conf.setInt("mapreduce.local.map.tasks.maximum", 16);
    conf.setInt("mapreduce.local.reduce.tasks.maximum", 16);
    Job job = Job.getInstance(conf, "testMapReduceInternal()-Job");
    TableMapReduceUtil.initTableMapperJob(TABLE_NAME, scan,
            RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);

    // Substituting standard TableInputFormat which was set in TableMapReduceUtil.initTableMapperJob(...)
    job.setInputFormatClass(WdTableInputFormat.class);
    keyDistributor.addInfo(job.getConfiguration());

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);

    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);

    long mapInputRecords = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS).getValue();
    Assert.assertEquals(valuesCountInSeekInterval, mapInputRecords);

    // Need to kill the job after completion, after it could leave MRAppMaster running not terminated.
    // Not sure what causing this, but maybe problem in MiniYarnCluster
    job.killJob();
  }

  /**
   * Mapper that runs the count.
   * NOTE: it was copied from RowCounter class
   */
  static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
    /** Counter enumeration to count the actual rows. */
    public enum Counters { ROWS }

    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
      for (Cell cell : values.rawCells()) {
        if (cell.getValueLength() > 0) {
          context.getCounter(Counters.ROWS).increment(1);
          break;
        }
      }
    }
  }
}
