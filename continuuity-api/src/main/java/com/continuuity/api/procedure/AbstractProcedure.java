/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

/**
 * An abstract implementation of the {@link Procedure} interface. This
 * class is preferrable to {@link Procedure} interface if you don't have
 * have complex logic during initialization and destoying of Procedure.
 *
 * <p>This class implements {@link #initialize(ProcedureContext)},
 * {@link #destroy()} and {@link #configure()}</p>
 *
 * @see Procedure
 */
public abstract class AbstractProcedure implements Procedure {

  private final String name;
  private ProcedureContext procedureContext;

  /**
   * Default constructor which uses {@link #getClass()}.{@link Class#getSimpleName() getSimpleName} as the
   * procedure name.
   */
  protected AbstractProcedure() {
    this.name = getClass().getSimpleName();
  }

  /**
   * Constructor that uses the given name as the procedure name.
   * @param name Name of the procedure.
   */
  protected AbstractProcedure(String name) {
    this.name = name;
  }

  /**
   * Default implementation of configure that returns a default {@link ProcedureSpecification}.
   * @return An instance of {@link ProcedureSpecification}
   */
  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  /**
   * Simpler implementation that memormizes the context of {@link Procedure}.
   * @param context An instance of {@link ProcedureContext}
   */
  @Override
  public void initialize(ProcedureContext context) {
    this.procedureContext = context;
  }

  /**
   * Destroy implementation that does nothing.
   */
  @Override
  public void destroy() {
    // Nothing to do
  }

  /**
   * @return An instance of {@link ProcedureContext} when this Procedure is running. Otherwise return
   *         {@code null} if it is not running or not yet initialized by the runtime environment.
   */
  public ProcedureContext getContext() {
    return procedureContext;
  }

  /**
   * @return Name of the {@link Procedure}
   */
  protected String getName() {
    return name;
  }

  /**
   * @return Description associated with the {@link Procedure}
   */
  protected String getDescription() {
    return String.format("Procedure for executing %s.", getName());
  }
}
