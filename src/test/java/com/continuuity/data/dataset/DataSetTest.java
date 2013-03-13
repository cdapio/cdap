package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.util.OperationUtil;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class DataSetTest extends DataSetTestBase {

  @BeforeClass
  public static void configure() {

    // a key value table
    DataSet table = new KeyValueTable("table");
    // an indexed table
    DataSet index = new IndexedTable("index", Bytes.toBytes("phone"));
    // a data set that lacks the runtime constructor
    DataSet incomplete = new IncompleteDataSet("dummy");
    // a data set that lacks the runtime constructor
    DataSet throwing = new ThrowingDataSet("badguy");

    setupInstantiator(Lists.newArrayList(table, index, incomplete, throwing));
  }

  @Test
  public void testToAndFromJSon() {
    for (DataSetSpecification spec : specs) {
      Gson gson = new Gson();
      String json = gson.toJson(spec);
      //System.out.println("JSON: " + json);
      DataSetSpecification spec1 = gson.fromJson(json, DataSetSpecification.class);
      Assert.assertEquals(spec, spec1);
    }
  }

  @Test(expected = ClassCastException.class)
  public void testInstantiateWrongClass() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("index");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testInstantiateNonExistent() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("fhdjkshgjkla");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testMissingConstructor() throws Exception {
    // try to instantiate the incomplete data set
    @SuppressWarnings("unused")
    IncompleteDataSet ds = instantiator.getDataSet("dummy");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testThrowingConstructor() throws Exception {
    // try to instantiate the incomplete data set
    @SuppressWarnings("unused")
    ThrowingDataSet ds = instantiator.getDataSet("badguy");
  }

  // a dummy executor that verifies the metrics name of operations
  class DummyOpex extends NoOperationExecutor {

    @Override
    public void commit(OperationContext context, WriteOperation op) throws OperationException {
      Assert.assertEquals("testtest", op.getMetricName());
    }
    @Override
    public Map<byte[], Long> increment(OperationContext context, Increment increment) throws OperationException {
      Assert.assertEquals("testtest", increment.getMetricName());
      Map<byte[], Long> result = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      for (byte[] column : increment.getColumns()) {
        result.put(column, 42L);
      }
      return result;
    }
  }

  // tests that nested tables inside datasets use the name of the top-level dataset as the metric name
  @Test
  public void testDataSetInstantiationWithMetricName() throws OperationException {
    // setup a dummy opex, transaction proxy and instantiator
    OperationExecutor opex = new DummyOpex();
    TransactionProxy proxy = new TransactionProxy();
    DataSetInstantiator inst = new DataSetInstantiator(new DataFabricImpl(opex, OperationUtil.DEFAULT), proxy, this.getClass().getClassLoader());

    // test with a single nested table (KeyValueTable embeds a Table with a modifeied name)
    DataSetSpecification spec = new KeyValueTable("testtest").configure();
    inst.setDataSets(Collections.singletonList(spec));
    KeyValueTable kvTable = inst.getDataSet("testtest");
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, OperationUtil.DEFAULT));
    kvTable.write("a".getBytes(), "b".getBytes());

    // test with a double nested table - a dataset that embeds a table and a kv table that in turn embeds another table
    spec = new DoubleNestedTable("testtest").configure();
    inst.setDataSets(Collections.singletonList(spec));
    DoubleNestedTable dn = inst.getDataSet("testtest");
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, OperationUtil.DEFAULT));
    dn.writeAndInc("a", 17);
  }
}
