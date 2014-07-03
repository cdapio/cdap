package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;

/**
 * Helper class for data set tests. It takes care of instantiating the
 * tx, the data fabric and the data set instantiator. A test case that
 * extends this class can define a @BeforeClass method that configures the
 * data sets to be used in the test, by calling setupInstantiator(datasets).
 *
 * The test base also provides methods to simulate transactions (as they
 * would occur in a flow or query provider). Call newCollector() to start a
 * collecting operations, and executeCollector() to perform all collected
 * operations as a transaction.
 */
public class DataSetTestBase {

  protected static DataFabric fabric;
  protected static DatasetFramework datasetFramework;
  protected static TransactionSystemClient txSystemClient;
  protected static CConfiguration configuration;

  protected static List<DataSetSpecification> specs;
  protected static DataSetInstantiator instantiator;

  /**
   * Sets up the in-memory data fabric.
   */
  @BeforeClass
  public static void setupDataFabric() {
    // use Guice to inject an in-memory tx
    final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                           new DataSetsModules().getInMemoryModule(),
                           new DiscoveryRuntimeModule().getInMemoryModules(),
                           new TransactionMetricsModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(LocationFactory.class).to(LocalLocationFactory.class);
                             }
                           });
    configuration = injector.getInstance(CConfiguration.class);
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    DataSetAccessor dataSetAccessor = injector.getInstance(DataSetAccessor.class);
    // and create a data fabric with the default operation context
    fabric = new DataFabric2Impl(locationFactory, dataSetAccessor);
    datasetFramework = injector.getInstance(DatasetFramework.class);
  }

  /**
   * Configures the data set instantiator with a single data set.
   * @param dataset the single data set used in the test.
   */
  public static void setupInstantiator(DataSet dataset) {
    setupInstantiator(Collections.singletonList(dataset));
  }

  /**
   * Configures the data set instantiator with a list of data sets.
   * @param datasets the data sets used in the test
   */
  public static void setupInstantiator(List<DataSet> datasets) {
    // configure all of the data sets
    specs = Lists.newArrayList();
    for (DataSet dataset : datasets) {
      specs.add(dataset.configure());
    }
    // create an instantiator the resulting list of data set specs
    instantiator = new DataSetInstantiator(fabric, datasetFramework, configuration, null);
    instantiator.setDataSets(specs, Collections.<DatasetCreationSpec>emptyList());
  }

  /**
   * Start a new transaction. This is similar to what a flowlet runner would do before
   * processing each data object. It creates a new transaction context, and all data sets
   * configured through the instantiator (@see #setupInstantiator) will start using that
   * transaction agent immediately.
   */
  public static TransactionContext newTransaction() throws Exception {
    TransactionContext txContext = new TransactionContext(txSystemClient,
                                                      instantiator.getTransactionAware());
    txContext.start();
    return txContext;
  }

  /**
   * finish the current transaction. This is similar to what a flowlet runner would do after
   * processing each data object. After the transaction finishes - whether successful or
   * unsuccessful - a new transaction agent should be started for subsequent operations.
   * @throws OperationException if the transaction fails for any reason
   */
  public static void commitTransaction(TransactionContext txContext) throws Exception {
    txContext.finish();
  }
}
