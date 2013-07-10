package com.continuuity.test;

/**
 * Instance of this class is for managing a running {@link com.continuuity.api.procedure.Procedure Procedure}.
 */
public interface ProcedureManager {

  /**
   * Stops the running procedure.
   */
  void stop();

  /**
   * @return A {@link ProcedureClient} for issuing queries to the running procedure.
   */
  ProcedureClient getClient();
}
