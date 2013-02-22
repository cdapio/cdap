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
   * Adds the response to be returned to the caller. Calling this method multiple times to the same
   * {@link ProcedureResponder} will return the same {@link ProcedureResponse.Writer} instance and the
   * latter submitted {@link ProcedureResponse} would be ignored.
   *
   * @param response An instance of {@link ProcedureResponse} containing the response to be returned.
   * @return  An instance of {@link ProcedureResponse.Writer} for writing data of the response.
   * @throws IOException When there is issue sending the response to the callee.
   */
  ProcedureResponse.Writer response(ProcedureResponse response) throws IOException;
}
