package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.app.program.Program;
import com.continuuity.data.operation.OperationContext;

/**
 * Factory for creating {@link DataSetContext}.
 */
public interface DataSetContextFactory {

  /**
   * Creats a {@link DataSetContext} with the given {@link OperationContext}.
   * @param program
   * @return
   */
  DataSetContext create(Program program);
}
