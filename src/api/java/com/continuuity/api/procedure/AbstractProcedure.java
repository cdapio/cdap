package com.continuuity.api.procedure;

/**
 *
 */
public abstract class AbstractProcedure implements Procedure {

  protected ProcedureContext procedureContext;

  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.builder()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  @Override
  public void initialize(ProcedureContext context) {
    this.procedureContext = context;
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  protected String getName() {
    return getClass().getSimpleName();
  }

  protected String getDescription() {
    return String.format("Procedure for executing %s.", getName());
  }
}
