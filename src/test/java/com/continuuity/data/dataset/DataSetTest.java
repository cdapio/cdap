package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

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

  class DummyOpex extends NoOperationExecutor {
    @Override
    public void commit(OperationContext context, WriteOperation op) throws OperationException {
      Assert.assertEquals("testtest", op.getMetricName());
    }
  }

  @Test
  public void testDataSetInstantiationWithMetricName() throws OperationException {
    OperationExecutor opex = new DummyOpex();
    TransactionProxy proxy = new TransactionProxy();
    DataSetInstantiator inst = new DataSetInstantiator(new DataFabricImpl(opex, OperationContext.DEFAULT), proxy, null);
    DataSetSpecification spec = new KeyValueTable("testtest").configure();
    inst.setDataSets(Collections.singletonList(spec));
    KeyValueTable kvTable = inst.getDataSet("testtest");
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, OperationContext.DEFAULT));
    kvTable.write("a".getBytes(), "b".getBytes());
  }

}
