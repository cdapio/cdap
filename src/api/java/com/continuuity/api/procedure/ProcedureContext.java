package com.continuuity.api.procedure;

import com.continuuity.api.data.DataSet;

/**
 * This interface represents the Procedure context, which consists of set of methods for
 * acquiring instance of {@link DataSet}
 */
public interface ProcedureContext {
  /**
   * Given a name of dataset, returns an instance of {@link DataSet}
   * @param name of the {@link DataSet}
   * @return An instance of {@link DataSet}
   */
  DataSet getDataSet(String name);
}
