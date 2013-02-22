/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Flowlet context, which consists of things like
 * number of instances of this flowlet and methods to create and initialize a
 * {@link DataSet} by name.
 */
public interface FlowletContext {

  /**
   * @return Number of instances of this flowlet.
   */
  int getInstanceCount();

  /**
   * @return Name of this flowlet.
   */
  String getName();

  /**
   * Gets an instance of {@link DataSet} with the given name.
   * @param name Name of the {@link DataSet}. It must be previously declared through the
   *             {@link FlowletSpecification.Builder.AfterDescription#useDataSet(String, String...) useDataSet} method
   *             during {@link com.continuuity.api.flow.flowlet.Flowlet#configure() configure} time or by
   *             using {@link com.continuuity.api.annotation.UseDataSet UseDataSet} annotation.
   * @param <T> Actual {@link DataSet} type.
   * @return A {@link DataSet} instance of type {@code T}.
   * @exception IllegalArgumentException When the given name is not referring to a configured {@link DataSet}.
   */
  <T extends DataSet> T getDataSet(String name);

  /**
   * @return The specification used to configure this {@link Flowlet} instance.
   */
  FlowletSpecification getSpecification();
}
