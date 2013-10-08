package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data2.transaction.TransactionContext;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Time series table tests.
 */
public class SimpleTimeseriesTableTest extends DataSetTestBase {
  private static SimpleTimeseriesTable table;

  @BeforeClass
  public static void configure() throws Exception {
    DataSet metricsTable = new SimpleTimeseriesTable("metricsTable");
    setupInstantiator(Collections.singletonList(metricsTable));
    table = instantiator.getDataSet("metricsTable");
  }

  // TODO: move it into base DataSetTestBase class?
  @Test
  public void testToAndFromJSon() {
    for (DataSetSpecification spec : specs) {
      Gson gson = new Gson();
      String json = gson.toJson(spec);
      DataSetSpecification spec1 = gson.fromJson(json, DataSetSpecification.class);
      Assert.assertEquals(spec, spec1);
    }
  }

  @Test
  public void testDataSet() throws Exception {

    TransactionContext txContext = newTransaction();

    byte[] metric1 = Bytes.toBytes("metric1");
    byte[] metric2 = Bytes.toBytes("metric2");
    byte[] tag1 = Bytes.toBytes("111");
    byte[] tag2 = Bytes.toBytes("22");
    byte[] tag3 = Bytes.toBytes("3");
    byte[] tag4 = Bytes.toBytes("123");

    long hour = 60 * 60 * 1000;
    long second = 1000;

    long ts = System.currentTimeMillis();

    // m1e1 = metric: 1, entity: 1
    SimpleTimeseriesTable.Entry m1e1 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(3L), ts, tag3, tag2, tag1);
    table.write(m1e1);
    SimpleTimeseriesTable.Entry m1e2 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(10L), ts + 2 * second, tag3);
    table.write(m1e2);
    SimpleTimeseriesTable.Entry m1e3 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(15L), ts + 2 * hour, tag1);
    table.write(m1e3);
    SimpleTimeseriesTable.Entry m1e4 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(23L), ts + 3 * hour, tag2, tag3);
    table.write(m1e4);
    SimpleTimeseriesTable.Entry m1e5 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(55L), ts + 3 * hour + 2 * second);
    table.write(m1e5);

    SimpleTimeseriesTable.Entry m2e1 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(4L), ts);
    table.write(m2e1);
    SimpleTimeseriesTable.Entry m2e2 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(11L), ts + 2 * second, tag2);
    table.write(m2e2);
    SimpleTimeseriesTable.Entry m2e3 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(16L), ts + 2 * hour, tag2);
    table.write(m2e3);
    SimpleTimeseriesTable.Entry m2e4 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(24L), ts + 3 * hour, tag1, tag3);
    table.write(m2e4);
    SimpleTimeseriesTable.Entry m2e5 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(56L), ts + 3 * hour + 2 * second,
                                                            tag3, tag1);
    table.write(m2e5);

    // whole interval is searched
    assertReadResult(table.read(metric1, ts, ts + 5 * hour), m1e1, m1e2, m1e3, m1e4, m1e5);
    assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag2), m1e1, m1e4);
    assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag4));
    assertReadResult(table.read(metric1, ts, ts + 5 * hour, tag2, tag4));
    // This is extreme case, should not be really used by anyone. Still we want to test that it won't fail. It returns
    // nothing because there's hard limit on the number of rows traversed during the read.
    assertReadResult(table.read(metric1, 0, Long.MAX_VALUE));

    // part of the interval
    assertReadResult(table.read(metric1, ts + second, ts + 2 * second), m1e2);
    assertReadResult(table.read(metric1, ts + hour, ts + 3 * hour), m1e3, m1e4);
    assertReadResult(table.read(metric1, ts + second, ts + 3 * hour), m1e2, m1e3, m1e4);
    assertReadResult(table.read(metric1, ts + second, ts + 3 * hour, tag3), m1e2, m1e4);
    assertReadResult(table.read(metric1, ts + second, ts + 3 * hour, tag3, tag2), m1e4);

    // different metric
    assertReadResult(table.read(metric2, ts + hour, ts + 3 * hour, tag2), m2e3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeRangeCondition() throws Exception {
    TransactionContext txContext = newTransaction();
    long ts = System.currentTimeMillis();
    table.read(Bytes.toBytes("any"), ts, ts - 100);
  }

  @Test
  public void testValidTimeRangesAreAllowed() throws Exception {
    long ts = System.currentTimeMillis();
    table.read(Bytes.toBytes("any"), ts, ts);
    table.read(Bytes.toBytes("any"), ts, ts + 100);
  }

  // TODO: test for wrong params: end time less than start time
  private void assertReadResult(List<SimpleTimeseriesTable.Entry> result, SimpleTimeseriesTable.Entry... entries) {
    Assert.assertEquals(entries.length, result.size());
    for (int i = 0; i < entries.length; i++) {
      assertEquals(entries[i], result.get(i));
    }
  }

  private void assertEquals(final SimpleTimeseriesTable.Entry left, final SimpleTimeseriesTable.Entry right) {
    Assert.assertArrayEquals(left.getKey(), right.getKey());
    Assert.assertEquals(left.getTimestamp(), right.getTimestamp());
    Assert.assertArrayEquals(left.getValue(), right.getValue());
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
}
