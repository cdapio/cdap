package com.continuuity.data.dataset;

import com.continuuity.api.data.*;
import com.continuuity.data.BatchCollectionClient;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.SimpleBatchCollectionClient;
import com.continuuity.data.operation.SimpleBatchCollector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;

/**
 * Helper class for data set tests. It takes care of instantiating the
 * opex, the data fabric and the data set instantiator. A test case that
 * extends this class can define a @BeforeClass method that configures the
 * data sets to be used in the test, by calling setupInstantiator(datasets).
 *
 * The test base also provides methods to simulate transactions (as they
 * would occur in a flow or query provider). Call newCollector() to start a
 * collecting operations, and executeCollector() to perform all collected
 * operations as a transaction.
 */
public class DataSetTestBase {

  private static DataFabric fabric;
  private static SimpleBatchCollector collector = new SimpleBatchCollector();
  private static BatchCollectionClient collectionClient = new SimpleBatchCollectionClient();

  protected static List<DataSetSpecification> specs;
  protected static DataSetInstantiator instantiator;

  /**
   * Sets up the in-memory operation executor and the data fabric
   */
  @BeforeClass
  public static void setupDataFabric() {
    // use Guice to inject an in-memory opex
    final Injector injector =
      // Guice.createInjector(new DataFabricModules().getInMemoryModules());
      Guice.createInjector(new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    OperationExecutor executor = injector.getInstance(OperationExecutor.class);
    // and create a data fabric with the default operation context
    fabric = new DataFabricImpl(executor, OperationContext.DEFAULT);
  }

  /**
   * Configures the data set instantiator with a single data set
   * @param dataset the single data set used in the test
   */
  public static void setupInstantiator(DataSet dataset) {
    setupInstantiator(Collections.singletonList(dataset));
  }

  /**
   * Configures the data set instantiator with a list of data sets
   * @param datasets the data sets used in the test
   */
  public static void setupInstantiator(List<DataSet> datasets) {
    // configure all of the data sets
    specs = Lists.newArrayList();
    for (DataSet dataset : datasets) {
      specs.add(dataset.configure());
    }
    // create an instantiator the resulting list of data set specs
    instantiator = new DataSetInstantiator(fabric, collectionClient, null);
    instantiator.setDataSets(specs);
  }

  /**
   * Start a new batch collection. This is similar to what a flowlet runner
   * would do before processing each tuple. It creates a new batch collector
   * and all data sets configured through the instantiator (@see
   * #setupInstantiator) will start using that collector immediately.
   */
  public static void newCollector() {
    collector = new SimpleBatchCollector();
    collectionClient.setCollector(collector);
  }

  /**
   * Execute the collected operations in a transaction. This is similar to
   * what a flowlet runner would do after processing each tuple. After the
   * transaction finishes - whether successful or unsuccessful - a new
   * batch collection is started (@see #newCollector).
   * @throws OperationException if the transaction fails for any reason
   */
  public static void executeCollector() throws OperationException {
    try {
      fabric.execute(collector.getWrites());
    } finally {
      newCollector();
    }
  }
}
