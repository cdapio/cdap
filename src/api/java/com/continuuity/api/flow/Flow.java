package com.continuuity.api.flow;

/**
 * Flow is a collection of {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} that are
 * wired together into a DAG (Direct Acylic Graph).
 *
 * <p>
 *   To create a Flow, one must implement this interface. The {@link #configure()} method will be
 *   invoked during deployment time and it returns a {@link FlowSpecification} to specify how to
 *   configure the given Flow.
 * </p>
 *
 * @see com.continuuity.api.flow.flowlet.Flowlet Flowlet
 */
public interface Flow {

  /**
   * Configure the {@link Flow} by returning an {@link FlowSpecification}.
   *
   * @return An instance of {@link FlowSpecification}.
   */
  FlowSpecification configure();
}
