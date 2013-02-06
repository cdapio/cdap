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

  protected ProcedureContext procedureContext;

  /**
   * Default implementation of configure that returns a default {@link ProcedureSpecification}
   * @return An instance of {@link ProcedureSpecification}
   */
  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.builder()
      .setName(getName())
      .setDescription(getDescription())
      .build();
  }

  /**
   * Simpler implementation that memormizes the context of {@link Procedure}
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
   * @return Name of the {@link Procedure}
   */
  protected String getName() {
    return getClass().getSimpleName();
  }

  /**
   * @return Description associated with the {@link Procedure}
   */
  protected String getDescription() {
    return String.format("Procedure for executing %s.", getName());
  }
}
