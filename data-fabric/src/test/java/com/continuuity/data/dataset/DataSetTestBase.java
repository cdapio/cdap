package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.executor.BatchTransactionAgentWithSyncReads;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.util.OperationUtil;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
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

  private static OperationExecutor opex;
  protected static DataFabric fabric;

  private static TransactionAgent agent;
  protected static final TransactionProxy PROXY = new TransactionProxy();

  protected static List<DataSetSpecification> specs;
  protected static DataSetInstantiator instantiator;

  /**
   * Enum for the transaction agent mode.
   */
  protected enum Mode { Sync, Batch, Smart }
  /**
   * Sets up the in-memory operation executor and the data fabric.
   */
  @BeforeClass
  public static void setupDataFabric() {
    // use Guice to inject an in-memory opex
    final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(LocationFactory.class).to(LocalLocationFactory.class);
                             }
                           });
    opex = injector.getInstance(OperationExecutor.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    // and create a data fabric with the default operation context
    fabric = new DataFabricImpl(opex, locationFactory, OperationUtil.DEFAULT);
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
    instantiator = new DataSetInstantiator(fabric, PROXY, null);
    instantiator.setDataSets(specs);
  }

  /**
   * Start a new transaction. This is similar to what a flowlet runner would do before
   * processing each data object. It creates a new transaction agent, and all data sets
   * configured through the instantiator (@see #setupInstantiator) will start using that
   * transaction agent immediately.
   */
  public static void newTransaction(Mode mode) throws OperationException {
    switch (mode) {
      case Sync: agent = new SynchronousTransactionAgent(opex, OperationUtil.DEFAULT); break;
      case Batch: agent = new BatchTransactionAgentWithSyncReads(opex, OperationUtil.DEFAULT); break;
      case Smart: agent = new SmartTransactionAgent(opex, OperationUtil.DEFAULT);
    }
    agent.start();
    PROXY.setTransactionAgent(agent);
  }

  /**
   * finish the current transaction. This is similar to what a flowlet runner would do after
   * processing each data object. After the transaction finishes - whether successful or
   * unsuccessful - a new transaction agent should be started for subsequent operations.
   * @throws OperationException if the transaction fails for any reason
   */
  public static void commitTransaction() throws OperationException {
    agent.finish();
  }
}
