/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import java.io.IOException;

/**
 * This interface defines a response generator.
 */
public interface ProcedureResponder {
  /**
   * Adds to the response to be returned to the caller.
   *
   * @param response An instance of {@link ProcedureResponse} containing the response to be returned.
   * @return  An instance of {@link ProcedureResponse.Writer} for writing data of the response.
   * @throws IOException When there is issue sending the response to the callee.
   */
  ProcedureResponse.Writer response(ProcedureResponse response) throws IOException;
}
