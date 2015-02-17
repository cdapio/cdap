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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Time series table tests.
 */
public class TimeseriesTableTest extends AbstractDatasetTest {
  private static final Id.DatasetInstance metricsTableInstance = Id.DatasetInstance.from(NAMESPACE_ID, "metricsTable");
  private static TimeseriesTable table;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createInstance("timeseriesTable", metricsTableInstance, DatasetProperties.EMPTY);
    table = getInstance(metricsTableInstance);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance(metricsTableInstance);
  }

  @Test
  public void testDataSet() throws Exception {

    TransactionExecutor txnl = newTransactionExecutor(table);

    // this test runs all operations synchronously
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        byte[] metric1 = Bytes.toBytes("metric1");
        byte[] metric2 = Bytes.toBytes("metric2");
        byte[] tag1 = Bytes.toBytes("111");
        byte[] tag2 = Bytes.toBytes("22");
        byte[] tag3 = Bytes.toBytes("3");
        byte[] tag4 = Bytes.toBytes("123");

        long hour = TimeUnit.HOURS.toMillis(1);
        long second = TimeUnit.SECONDS.toMillis(1);

        long ts = System.currentTimeMillis();

        // m1e1 = metric: 1, entity: 1
        TimeseriesTable.Entry m1e1 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(3L), ts, tag3, tag2, tag1);
        table.write(m1e1);
        TimeseriesTable.Entry m1e2 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(10L), ts + 2 * second, tag3);
        table.write(m1e2);
        TimeseriesTable.Entry m1e3 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(15L), ts + 2 * hour, tag1);
        table.write(m1e3);
        TimeseriesTable.Entry m1e4 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(23L), ts + 3 * hour, tag2, tag3);
        table.write(m1e4);
        TimeseriesTable.Entry m1e5 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(55L), ts + 3 * hour + 2 * second);
        table.write(m1e5);

        TimeseriesTable.Entry m2e1 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(4L), ts);
        table.write(m2e1);
        TimeseriesTable.Entry m2e2 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(11L), ts + 2 * second, tag2);
        table.write(m2e2);
        TimeseriesTable.Entry m2e3 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(16L), ts + 2 * hour, tag2);
        table.write(m2e3);
        TimeseriesTable.Entry m2e4 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(24L), ts + 3 * hour, tag1, tag3);
        table.write(m2e4);
        TimeseriesTable.Entry m2e5 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(56L), ts + 3 * hour + 2 * second,
                                                               tag3, tag1);
        table.write(m2e5);

        // whole interval is searched
        assertReadResult(table.read(metric1, ts, ts + 5 * hour), m1e1, m1e2, m1e3, m1e4, m1e5);
        assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag2), m1e1, m1e4);
        assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag4));
        assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag2, tag4));
        // This is extreme case, should not be really used by anyone. Still we want to test that it won't fail.
        // It returns nothing because there's hard limit on the number of rows traversed during the read.
        assertReadResult(table.read(metric1, 0, Long.MAX_VALUE));

        // test pagination read
        assertReadResult(table.read(metric1, ts, ts + 5 * hour, 1, 2), m1e2, m1e3);

        // part of the interval
        assertReadResult(table.read(metric1, ts + second, ts + 2 * second), m1e2);
        assertReadResult(table.read(metric1, ts + hour, ts + 3 * hour), m1e3, m1e4);
        assertReadResult(table.read(metric1, ts + second, ts + 3 * hour), m1e2, m1e3, m1e4);
        assertReadResult(table.read(metric1, ts + second, ts + 3 * hour, tag3), m1e2, m1e4);
        assertReadResult(table.read(metric1, ts + second, ts + 3 * hour, tag3, tag2), m1e4);

        // different metric
        assertReadResult(table.read(metric2, ts + hour, ts + 3 * hour, tag2), m2e3);
      }
    });
  }

  @Test(expected = TransactionFailureException.class)
  public void testInvalidTimeRangeCondition() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        long ts = System.currentTimeMillis();
        table.read(Bytes.toBytes("any"), ts, ts - 100);
      }
    });
  }

  @Test
  public void testValidTimeRangesAreAllowed() throws Exception {
    TransactionExecutor txnl = newTransactionExecutor(table);
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        long ts = System.currentTimeMillis();
        Iterator<TimeseriesTable.Entry> temp = table.read(Bytes.toBytes("any"), ts, ts);
        Assert.assertFalse(temp.hasNext());
        temp = table.read(Bytes.toBytes("any"), ts, ts + 100);
        Assert.assertFalse(temp.hasNext());
      }
    });
  }

  // TODO: test for wrong params: end time less than start time
  private void assertReadResult(Iterator<TimeseriesTable.Entry> resultIterator, TimeseriesTable.Entry... entries) {
    int count = 0;
    while (resultIterator.hasNext()) {

      assertEquals(entries[count], resultIterator.next());
      count++;
    }
    Assert.assertEquals(entries.length, count);
  }

  private void assertEquals(final TimeseriesTable.Entry left, final TimeseriesTable.Entry right) {
    Assert.assertArrayEquals(left.getKey(), right.getKey());
    Assert.assertEquals(left.getTimestamp(), right.getTimestamp());
    Assert.assertArrayEquals(left.getValue(), right.getValue());
    assertEqualsIgnoreOrder(left.getTags(), right.getTags());
  }

  private void assertEqualsIgnoreOrder(final byte[][] left, final byte[][] right) {
    Arrays.sort(left, Bytes.BYTES_COMPARATOR);
    Arrays.sort(right, Bytes.BYTES_COMPARATOR);
    Assert.assertEquals(left.length, right.length);
    for (int i = 0; i < left.length; i++) {
      Assert.assertArrayEquals(left[i], right[i]);
    }
  }

  /**
   * Time series table format test.
   */
  @Test
  public void testColumnNameFormat() {
    // Note: tags are sorted lexographically
    byte[] tag1 = Bytes.toBytes("111");
    byte[] tag2 = Bytes.toBytes("22");
    byte[] tag3 = Bytes.toBytes("3");

    long ts = System.currentTimeMillis();
    byte[][] tags = { tag1, tag2, tag3 };
    byte[] columnName = TimeseriesTable.createColumnName(ts, tags);

    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertTrue(TimeseriesTable.hasTags(columnName));
  }

  @Test
  public void testColumnNameFormatWithNoTags() {
    long ts = System.currentTimeMillis();
    byte[] columnName = TimeseriesTable.createColumnName(ts, new byte[0][]);
    Assert.assertEquals(ts, TimeseriesTable.parseTimeStamp(columnName));
    Assert.assertFalse(TimeseriesTable.hasTags(columnName));
  }

  @Test
  public void testRowFormat() {
    // 1 min
    long timeIntervalPerRow = TimeUnit.MINUTES.toMillis(1);
    byte[] key = Bytes.toBytes("key");

    long ts1 = timeIntervalPerRow + 1;

    // These are the things we care about:

    // * If the timestamps fall into same time interval then row keys are same
    Assert.assertArrayEquals(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                             TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow / 2, timeIntervalPerRow));
    // * If the timestamps don't fall into same time interval then row keys are different
    Assert.assertFalse(Arrays.equals(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                     TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 2, timeIntervalPerRow)));

    // * If timestamp A > timestamp B then row key A > row key B (we will have some optimization logic based on that)
    Assert.assertTrue(
      Bytes.compareTo(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                      TimeseriesTable.createRow(key, ts1 + timeIntervalPerRow * 5, timeIntervalPerRow)) < 0);

    // * For different keys the constructed rows are different
    byte[] key2 = Bytes.toBytes("KEY2");
    Assert.assertFalse(Arrays.equals(TimeseriesTable.createRow(key, ts1, timeIntervalPerRow),
                                     TimeseriesTable.createRow(key2, ts1, timeIntervalPerRow)));

    // * We can get all possible rows for a given time interval with getRowsForInterval
    List<byte[]> rows = new ArrayList<byte[]>();
    long startTime = timeIntervalPerRow * 4;
    long endTime = timeIntervalPerRow * 100;
    for (long i = startTime; i <= endTime; i += timeIntervalPerRow / 10) {
      byte[] row = TimeseriesTable.createRow(key, i, timeIntervalPerRow);
      if (rows.isEmpty() || !Arrays.equals(rows.get(rows.size() - 1), row)) {
        rows.add(row);
      }
    }

    int intervalsCount = (int) TimeseriesTable.getTimeIntervalsCount(startTime, endTime, timeIntervalPerRow);
    byte[][] rowsForInterval = new byte[intervalsCount][];
    for (int i = 0; i < intervalsCount; i++) {
      rowsForInterval[i] = TimeseriesTable.getRowOfKthInterval(key, startTime, i, timeIntervalPerRow);
    }
    assertEquals(rows.toArray(new byte[rows.size()][]), rowsForInterval);

    // same requirement, one more test (covers some bug I fixed)
    Assert.assertEquals(2, TimeseriesTable.getTimeIntervalsCount(timeIntervalPerRow + 1, timeIntervalPerRow * 2 + 1,
                                                                 timeIntervalPerRow));
  }

  private void assertEquals(final byte[][] left, final byte[][] right) {
    Assert.assertEquals(left.length, right.length);
    for (int i = 0; i < left.length; i++) {
      Assert.assertArrayEquals(left[i], right[i]);
    }
  }
}
