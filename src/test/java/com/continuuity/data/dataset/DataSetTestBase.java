package com.continuuity.data.dataset;

import com.continuuity.api.data.*;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.SimpleBatchCollectionClient;
import com.continuuity.data.operation.SimpleBatchCollector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;

public class DataSetTestBase {

  static OperationExecutor executor;
  static DataFabric fabric;
  static SimpleBatchCollector collector = new SimpleBatchCollector();
  static BatchCollectionClient collectionClient = new SimpleBatchCollectionClient();
  static DataSetInstantiator instantiator;

  @BeforeClass
  public static void setupDataFabric() {
    final Injector injector =
        Guice.createInjector(new DataFabricModules().getInMemoryModules());

    executor = injector.getInstance(OperationExecutor.class);
    fabric = new DataFabricImpl(executor, OperationContext.DEFAULT);
  }

  public static void setupInstantiator(DataSet dataset) {
    setupInstantiator(Collections.singletonList(dataset));
  }

  public static void setupInstantiator(List<DataSet> datasets) {
    // configure a couple of data sets
    List<DataSetSpecification> specs = Lists.newArrayList();
    for (DataSet dataset : datasets) {
      specs.add(dataset.configure());
    }
    // create an app context for running a procedure
    instantiator = new DataSetInstantiator();
    instantiator.setDataFabric(fabric);
    instantiator.setBatchCollectionClient(collectionClient);
    instantiator.setDataSets(specs);
  }

  public static void newCollector() {
    collector = new SimpleBatchCollector();
    collectionClient.setCollector(collector);
  }

  public static void executeCollector() throws OperationException {
    try {
      fabric.execute(collector.getWrites());
    } finally {
      newCollector();
    }
  }
}
