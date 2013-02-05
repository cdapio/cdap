package com.continuuity.api.procedure;

/**
 *
 */
public interface Procedure {

  ProcedureSpecification configure();

  void initialize(ProcedureContext context);

  void destroy();
}
