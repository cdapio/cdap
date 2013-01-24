package com.continuuity.api.data.set;

import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.util.Bytes;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.SimpleBatchCollectionClient;
import com.continuuity.data.operation.SimpleBatchCollector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TimeseriesTableTest {
  private static TimeseriesTable table;
  static SimpleBatchCollectionClient collectionClient = new SimpleBatchCollectionClient();
  static OperationExecutor executor;
  static DataFabric fabric;
  static List<DataSetSpecification> specs;
  static DataSetInstantiator instantiator;

  @BeforeClass
  public static void setup() throws Exception {
    final Injector injector =
        Guice.createInjector(new DataFabricModules().getInMemoryModules());

    executor = injector.getInstance(OperationExecutor.class);
    fabric = new DataFabricImpl(executor, OperationContext.DEFAULT);

    // configure a couple of data sets
    specs = Lists.newArrayList(new TimeseriesTable("metricsTable").configure());

    // create an app context for running a procedure
    instantiator = new DataSetInstantiator();
    instantiator.setDataFabric(fabric);
    instantiator.setBatchCollectionClient(collectionClient);
    instantiator.setDataSets(specs);

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
    SimpleBatchCollector collector = new SimpleBatchCollector();
    collectionClient.setCollector(collector);

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
    TimeseriesTable.Entry m1e1 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(3L), ts, tag3, tag2, tag1);
    table.write(m1e1);
    TimeseriesTable.Entry m1e2 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(10L), ts + 2 * second, tag3);
    table.write(m1e2);
    TimeseriesTable.Entry m1e3 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(15L), ts + 2 * hour, tag1);
    table.write(m1e3);
    TimeseriesTable.Entry m1e4 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(23L), ts + 3 * hour, tag2, tag3);
    table.write(m1e4);
    TimeseriesTable.Entry m1e5 = new TimeseriesTable.Entry(metric1, Bytes.toBytes(55L), ts + 3 * hour + 1 * second);
    table.write(m1e5);

    TimeseriesTable.Entry m2e1 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(4L), ts);
    table.write(m2e1);
    TimeseriesTable.Entry m2e2 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(11L), ts + 2 * second, tag3);
    table.write(m2e2);
    TimeseriesTable.Entry m2e3 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(16L), ts + 2 * hour, tag1);
    table.write(m2e3);
    TimeseriesTable.Entry m2e4 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(24L), ts + 3 * hour, tag2, tag3);
    table.write(m2e4);
    TimeseriesTable.Entry m2e5 = new TimeseriesTable.Entry(metric2, Bytes.toBytes(56L), ts + 3 * hour + 1 * second,
                                                            tag3, tag1);
    table.write(m2e5);

    assertReadResult(table.read(metric1, ts, ts + 5 * hour), m1e1, m1e2, m1e3, m1e4, m1e5);

  }

  private void assertReadResult(List<TimeseriesTable.Entry> result, TimeseriesTable.Entry... entries) {
    Assert.assertEquals(entries.length, result.size());
    for (int i = 0; i < entries.length; i++) {
      assertEquals(entries[i], result.get(i));
    }
  }

  private void assertEquals(final TimeseriesTable.Entry left, final TimeseriesTable.Entry right) {
    Assert.assertArrayEquals(left.getKey(), right.getKey());
    Assert.assertEquals(left.getTimestamp(), right.getTimestamp());
    Assert.assertArrayEquals(left.getValue(), right.getValue());
    Assert.assertArrayEquals(left.getValue(), right.getValue());
    assertEquals(left.getTags(), right.getTags());
  }

  private void assertEquals(final byte[][] left, final byte[][] right) {
    Assert.assertEquals(left.length, right.length);
    for (int i = 0; i < left.length; i++) {
      Assert.assertArrayEquals(left[i], right[i]);
    }
  }
}
