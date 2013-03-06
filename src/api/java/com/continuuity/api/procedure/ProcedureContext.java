/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Procedure context, which consists of set of methods for
 * acquiring instances of {@link DataSet}.
 */
public interface ProcedureContext {

  /**
   * Given a name of dataset, returns an instance of {@link DataSet}.
   * @param name of the {@link DataSet}.
   * @param <T> The specific {@link DataSet} type requested.
   * @return An instance of {@link DataSet}.
   */
  <T extends DataSet> T getDataSet(String name);

  /**
   * @return The specification used to configure this {@link Procedure} instance.
   */
  ProcedureSpecification getSpecification();
}
