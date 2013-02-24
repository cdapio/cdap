package com.continuuity.internal.app.services.legacy;

/**
 * ConnectionDefinition provides information about a single edge in the DAG.
 * It provides specification about how the flowlets are connected together.
 */
public interface ConnectionDefinition {
  /**
   * Returns the endpoint for an edge starting from a flowlet.
   * @return definition of edge starting from a flowlet.
   */
  FlowletStreamDefinition getFrom();

  /**
   * Returns the endpoint for an edge ending at a flowlet.
   * @return definition of edge ending at a flowlet.
   */
  FlowletStreamDefinition getTo();
}
