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
import com.google.common.base.Objects;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DataSetTest {

  static class ApplicationSpec {

    private String name;
    private List<DataSetSpecification> datasets = new LinkedList<DataSetSpecification>();

    public ApplicationSpec dataset(DataSet dataset) {
      this.datasets.add(dataset.configure().create());
      return this;
    }

    public ApplicationSpec name(String name) {
      this.name = name;
      return this;
    }

    public List<DataSetSpecification> getDatasets() {
      return this.datasets;
    }

    public String getName() {
      return name;
    }
  }

  static public class MyApplication {

    public ApplicationSpec configure() {

      // a key value table mapping name to phone number
      KeyValueTable numbers = new KeyValueTable("phoneTable");

      // an indexed tables allowing forward and reverse lookup
      IndexedTable idxNumbers = new IndexedTable("phoneIndex",
          new byte[] { 'p', 'h', 'o', 'n', 'e' });

      return new ApplicationSpec().
          name("phoneNumbers").
          dataset(numbers).
          dataset(idxNumbers);
    }
  }

  /** sample procedure (query provider) to illustrate the use of data sets */
  public static class MyProcedure {

    KeyValueTable numbers;
    IndexedTable idxNumbers;

    static final byte[] phoneCol = { 'p', 'h', 'o', 'n', 'e' };
    static final byte[] nameCol = { 'n', 'a', 'm', 'e' };

    public void initialize(ExecutionContext cxt)
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

  private static void assertEquals(Object expected, Object actual) {
    if (!Objects.equal(expected, actual)) {
      System.err.println("Error: expected " +
          (expected == null ? "<null>" : expected.toString())
          + " but actual is " +
          (actual == null ? "<null>" : actual.toString()));
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    final Injector injector =
        Guice.createInjector(new DataFabricModules().getInMemoryModules());

    executor = injector.getInstance(OperationExecutor.class);
    fabric = new DataFabricImpl(executor, OperationContext.DEFAULT);

    // create an app and let it configure itself
    MyApplication app = new MyApplication();
    ApplicationSpec spec = app.configure();

    // create an app context for running a procedure
    ExecutionContextImpl appContext = new ExecutionContextImpl();
    appContext.setDataFabric(fabric);
    appContext.setBatchCollectionClient(collectionClient);
    appContext.setDataSets(spec.getDatasets());

    // create a procedure and configure it
    proc.initialize(appContext);
  }

  @Test
  public void testDataSet() throws Exception {
    // try some procedure methods
    doOne("billy", "408-123-4567");
    doOne("jimmy", "415-987-4321");
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
