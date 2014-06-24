/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data;

import com.google.common.base.Preconditions;

import java.io.Closeable;

/**
 * This is the abstract base class for all datasets. A dataset is an
 * implementation of a data pattern that can be reused across programs and
 * applications. The life cycle of a dataset is as follows:
 * <li>An application declares the datasets that its programs will use.</li>
 * <li>When the application is deployed, the DataSet object is created and
 *   its configure() method is called. This method returns a specification
 *   that contains all information needed to instantiate the dataset at
 *   runtime.</li>
 * <li>At runtime (in a flow or procedure), the dataset is instantiated
 *   by dependency injection using the @UseDataSet annotation. This uses the
 *   constructor of the the dataset that takes the above specification
 *   as an argument. It is important that the dataset is instantiated through
 *   the context: this also makes sure that the DataFabric runtime is
 *   properly injected into the dataset.
 *   </li>
 * <li>Hence every DataSet must implement a configure() method and a
 *   constructor from DataSetSpecification.</li>
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.Dataset}
 *             and {@link com.continuuity.api.dataset.DatasetDefinition}
 */
@Deprecated
public abstract class DataSet implements Closeable {

  // the name of the data set (instance)
  private String name;
  private DataSetContext context;

  /**
   * Get the name of this data set.
   * @return the name of the data set
   */
  public final String getName() {
    return this.name;
  }

  /**
   * Base constructor that only sets the name of the data set.
   * @param name the name of the data set
   */
  public DataSet(String name) {
    this.name = name;
  }

  /**
   * This method is called at deployment time and must return the complete
   * specification that is needed to instantiate the data set at runtime
   * (@see #DataSet(DataSetSpecification)).
   * @return a data set spec that has all meta data needed for runtime
   *         instantiation
   */
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).create();
  }

  /**
   * This method is called at execution time given the same {@link DataSetSpecification} as returned by
   * the {@link #configure()}. When overriding this method,
   * calling {@link #initialize(DataSetSpecification, DataSetContext) super.initialize(spec)} is necessary
   * for super class initialization. No data operation inside this method as the DataSet is still
   * initializing.
   *
   * @param spec A {@link DataSetSpecification} which is the same as the one returned by {@link #configure()}.
   * @param context A {@link DataSetContext} for accessing DataSet in runtime.
   */
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    this.name = spec.getName();
    this.context = context;
  }

  /**
   * This method is called at runtime to release all resources that the dataset may have acquired.
   * After this is called, it is guaranteed that this instance of the dataset will not be used any more.
   * The base implementation is to do nothing.
   */
  @Override
  public void close() {
  }

  /**
   * Returns the runtime {@link DataSetContext}.
   *
   * @return An instance of {@link DataSetContext}
   * @throws IllegalStateException if the DataSet is not yet initialized.
   */
  protected DataSetContext getContext() {
    Preconditions.checkState(context != null, "DataSet is not initialized.");
    return context;
  }
}
