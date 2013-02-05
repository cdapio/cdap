package com.continuuity.api.procedure;

import com.continuuity.api.data.DataSet;

/**
 *
 */
public interface ProcedureContext {

  DataSet getDataSet(String name);
}
