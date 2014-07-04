package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;

import java.io.Closeable;

/**
 * The data set instantiator creates instances of data sets at runtime. It
 * must be called from the execution context to get operational instances
 * of data sets. Given a list of data set specs and a data fabric runtime it
 * can construct an instance of a data set and inject the data fabric runtime
 * into its base tables (and other built-in data sets).
 *
 * The instantiation and injection uses Java reflection a lot. This may look
 * unclean, but it helps us keep the DataSet API clean and simple (no need
 * to pass in data fabric runtime, no exposure of the developer to the raw
 * data fabric, he only interacts with data sets).
 */
public class DataSetInstantiator extends DataSetInstantiationBase implements DataSetContext {

  private final DataFabric fabric;
  private final DatasetFramework datasetFramework;

  /**
   * Constructor from data fabric.
   * @param fabric the data fabric
   * @param classLoader the class loader to use for loading data set classes.
   *                    If null, then the default class loader is used
   */
  public DataSetInstantiator(DataFabric fabric, DatasetFramework datasetFramework,
                             CConfiguration configuration, ClassLoader classLoader) {
    super(configuration, classLoader);
    this.fabric = fabric;
    this.datasetFramework =
      new NamespacedDatasetFramework(datasetFramework,
                                     new ReactorDatasetNamespace(configuration, DataSetAccessor.Namespace.USER));
  }

  /**
   *  The main value of this class: Creates a new instance of a data set, as
   *  specified by the matching data set spec, and injects the data fabric
   *  runtime into the new data set.
   *  @param dataSetName the name of the data set to instantiate
   */
  @Override
  public <T extends Closeable> T getDataSet(String dataSetName)
    throws DataSetInstantiationException {
    return (T) super.getDataSet(dataSetName, this.fabric, this.datasetFramework);
  }
}
