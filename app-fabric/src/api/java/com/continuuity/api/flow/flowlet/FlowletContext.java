/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.RuntimeContext;
import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Flowlet context, which consists of things like
 * number of instances of this flowlet and methods to create and initialize a
 * {@link DataSet} by name.
 */
public interface FlowletContext extends RuntimeContext {
  /**
   * @return Number of instances of this flowlet.
   */
  int getInstanceCount();

  /**
   * @return The instance id of this flowlet.
   */
  int getInstanceId();

  /**
   * @return Name of this flowlet.
   */
  String getName();

  /**
   * @return The specification used to configure this {@link Flowlet} instance.
   */
  FlowletSpecification getSpecification();
}
