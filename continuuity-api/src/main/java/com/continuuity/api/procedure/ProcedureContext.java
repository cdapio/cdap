/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import com.continuuity.api.RuntimeContext;
import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Procedure context, which consists of set of methods for
 * acquiring instances of {@link DataSet}.
 */
public interface ProcedureContext extends RuntimeContext {
  /**
   * @return The specification used to configure this {@link Procedure} instance.
   */
  ProcedureSpecification getSpecification();

  /**
   * @return number of instances for the procedure.
   */
  int getInstanceCount();
}
