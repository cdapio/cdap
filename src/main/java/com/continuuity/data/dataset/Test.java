package com.continuuity.data.dataset;

import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.base.Objects;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Test {

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

  private static void doOne(String name, String number) throws OperationException {
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

  public static void main(String[] args) throws Exception {

    final Injector injector =
        Guice.createInjector(new DataFabricModules().getInMemoryModules());

    executor = injector.getInstance(OperationExecutor.class);
    fabric = new DataFabricImpl(executor, OperationContext.DEFAULT);

    // create an app and let it configure itself
    MyApplication app = new MyApplication();
    ApplicationSpec spec = app.configure();

    // create an app context for running a procedure
    ApplicationContextImpl appContext = new ApplicationContextImpl();
    appContext.setDataFabric(fabric);
    appContext.setBatchCollectionClient(collectionClient);
    for (DataSetMeta meta : spec.getDatasets()) {
      appContext.addDataSet(meta);
    }

    // create a procedure and configure it
    proc.initialize(appContext);

    // try some procedure methods
    doOne("billy", "408-123-4567");
    doOne("jimmy", "415-987-4321");
  }

}
