package com.continuuity.data.dataset;

import com.continuuity.api.data.*;
import com.continuuity.api.data.set.IndexedTable;
import com.continuuity.api.data.set.KeyValueTable;
import com.continuuity.api.data.set.Table;
import com.continuuity.data.DataFabricImpl;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class DataSetTest {

  private static List<DataSetSpecification> configure() {

    // a key value table mapping name to phone number
    KeyValueTable numbers = new KeyValueTable("phoneTable");

    // an indexed tables allowing forward and reverse lookup
    IndexedTable idxNumbers = new IndexedTable("phoneIndex",
        new byte[] { 'p', 'h', 'o', 'n', 'e' });

    return Lists.newArrayList(
        numbers.configure(),
        idxNumbers.configure());
  }

  /** sample procedure to illustrate the use of data sets */
  public static class MyProcedure {

    KeyValueTable numbers;
    IndexedTable idxNumbers;

    static final byte[] phoneCol = { 'p', 'h', 'o', 'n', 'e' };
    static final byte[] nameCol = { 'n', 'a', 'm', 'e' };

    public void initialize(DataSetContext cxt)
        throws DataSetInstantiationException, OperationException {

      numbers = cxt.getDataSet("phoneTable");
      idxNumbers = cxt.getDataSet("phoneIndex");
    }

    public void addToPhonebook(String name, String phone)
        throws OperationException {
      byte[] key = name.getBytes();
      byte[] phon = phone.getBytes();
      byte[][] cols = { phoneCol, nameCol };
      byte[][] vals = { phon, key };

      numbers.write(key, phon);
      idxNumbers.write(new Table.Write(key, cols, vals));
    }

    public String getPhoneNumber(String name) throws OperationException {
      byte[] bytes = this.numbers.read(name.getBytes());
      return bytes == null ? null : new String(bytes);
    }

    public String getNameByNumber(String number) throws OperationException {
      OperationResult<Map<byte[],byte[]>> result =
          this.idxNumbers.readBy(new Table.Read(number.getBytes(), nameCol));
      if (!result.isEmpty()) {
        byte[] bytes = result.getValue().get(nameCol);
        if (bytes != null) {
          return new String(bytes);
        }
      }
      return null;
    }
  }

  static MyProcedure proc = new MyProcedure();
  static SimpleBatchCollectionClient collectionClient = new
      SimpleBatchCollectionClient();
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
    specs = configure();

    // create an app context for running a procedure
    instantiator = new DataSetInstantiator();
    instantiator.setDataFabric(fabric);
    instantiator.setBatchCollectionClient(collectionClient);
    instantiator.setDataSets(specs);

    // create a procedure and configure it
    proc.initialize(instantiator);
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

  @Test
  public void testDataSet() throws Exception {
    // try some procedure methods
    doOne("billy", "408-123-4567");
    doOne("jimmy", "415-987-4321");
  }

  @Test(expected = ClassCastException.class)
  public void testInstantiateWrongClass() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("phoneIndex");
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testInstantiateNonExistent() throws Exception {
    @SuppressWarnings("unused")
    KeyValueTable kvTable = instantiator.getDataSet("phonetab");
  }

  // this dataset is missing the constructor from data set spec
  static class Incomplete extends DataSet {
    public Incomplete(String name) {
      super(name);
    }
    @Override
    public DataSetSpecification configure() {
      return new DataSetSpecification.Builder(this).
          dataset(new Table("t_" + getName()).configure()).
          create();
    }
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testMissingConstructor() throws Exception {
    // configure an incomplete data set and add it to an instantiator
    DataSetSpecification spec = new Incomplete("dummy").configure();
    DataSetInstantiator inst = new DataSetInstantiator();
    inst.setDataFabric(fabric);
    inst.setBatchCollectionClient(collectionClient);
    inst.setDataSets(Collections.singletonList(spec));
    // try to instantiate the incomplete data set
    @SuppressWarnings("unused")
    Incomplete ds = inst.getDataSet("dummy");
  }

  // this class' data set spec constructor throws an exception
  static class Throwing extends Incomplete {
    public Throwing(String name) {
      super(name);
    }
    @SuppressWarnings("unused")
    public Throwing(DataSetSpecification spec) {
      super(spec.getName());
      throw new IllegalArgumentException("don't ever call me!");
    }
  }

  @Test(expected = DataSetInstantiationException.class)
  public void testThrowingConstructor() throws Exception {
    // configure an incomplete data set and add it to an instantiator
    DataSetSpecification spec = new Throwing("dummy").configure();
    DataSetInstantiator inst = new DataSetInstantiator();
    inst.setDataFabric(fabric);
    inst.setBatchCollectionClient(collectionClient);
    inst.setDataSets(Collections.singletonList(spec));
    // try to instantiate the incomplete data set
    @SuppressWarnings("unused")
    Throwing ds = inst.getDataSet("dummy");
  }

  private void doOne(String name, String number) throws OperationException {
    SimpleBatchCollector collector = new SimpleBatchCollector();
    collectionClient.setCollector(collector);
    proc.addToPhonebook(name, number);
    String number1 = proc.getPhoneNumber(name);
    assertEquals(number, number1);
    String name1 = proc.getNameByNumber(number);
    assertEquals(null, name1);
    executor.execute(OperationContext.DEFAULT, collector.getWrites());
    String name2 = proc.getNameByNumber(number);
    assertEquals(name, name2);
  }

}
