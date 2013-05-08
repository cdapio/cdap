package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.executor.TransactionProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data set instantiator creates instances of data sets at runtime. It
 * must be called from the execution context to get operational instances
 * of data sets. Given a list of data set specs and a data fabric runtime
 * (a data fabric and a batch collection client), it can construct an instance
 * of a data set and inject the data fabric runtime into its base tables (and
 * other built-in data sets).
 *
 * The instantiation and injection uses Java reflection a lot. This may look
 * unclean, but it helps us keep the DataSet API clean and simple (no need
 * to pass in data fabric runtime, no exposure of the developer to the raw
 * data fabric, he only interacts with data sets).
 */
public class DataSetInstantiator extends DataSetInstantiationBase implements DataSetContext {

  private static final Logger Log =
      LoggerFactory.getLogger(DataSetInstantiator.class);

  // the data fabric (an operation executor and an operation context
  private DataFabric fabric;
  // the transaction proxy
  private TransactionProxy transactionProxy;

  /**
   * Constructor from data fabric and transaction proxy.
   * @param fabric the data fabric
   * @param transactionProxy the transaction proxy to use for all data sets
   * @param classLoader the class loader to use for loading data set classes.
   *                    If null, then the default class loader is used
   */
  public DataSetInstantiator(DataFabric fabric,
                             TransactionProxy transactionProxy,
                             ClassLoader classLoader) {
    super(classLoader);
    this.fabric = fabric;
    this.transactionProxy = transactionProxy;
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   */
  public
  <T extends DataSet> T getDataSet(String dataSetName)
    throws DataSetInstantiationException {
    return super.getDataSet(dataSetName, this.fabric, this.transactionProxy);
  }
}
