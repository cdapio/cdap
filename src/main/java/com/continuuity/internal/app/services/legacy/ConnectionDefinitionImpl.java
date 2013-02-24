package com.continuuity.internal.app.services.legacy;

import com.google.common.base.Objects;

/**
 * Default implementation of ConnectionDefinition interface.
 */
public class ConnectionDefinitionImpl implements ConnectionDefinition {
  /**
   * Endpoint specification of an edge starting from a flowlet.
   */
  private FlowletStreamDefinitionImpl from;

  /**
   * Endpoint specification of an edge ending at a flowlet.
   */
  private FlowletStreamDefinitionImpl to;


  /**
   * Empty construction of object.
   */
  public ConnectionDefinitionImpl() {

  }

  /**
   * Construct the ConnectionDefinition object by specifying the endpoints of the edge connecting two flowlets.
   * @param from endpoint
   * @param to   endpoint.
   */
  public ConnectionDefinitionImpl(FlowletStreamDefinitionImpl from, FlowletStreamDefinitionImpl to) {
    this.from = from;
    this.to = to;
  }

  /**
   * Returns the endpoint for an edge starting from a flowlet.
   *
   * @return definition of edge starting from a flowlet.
   */
  @Override
  public FlowletStreamDefinition getFrom() {
    return from;
  }

  /**
   * Sets the specification of endpoint starting from a flowlet.
   *
   * @param from endpoint specification.
   */
  public void setFrom(FlowletStreamDefinitionImpl from) {
    this.from = from;
  }

  /**
   * Returns the endpoint for an edge ending at a flowlet.
   *
   * @return definition of edge ending at a flowlet.
   */
  @Override
  public FlowletStreamDefinition getTo() {
    return to;
  }

  /**
   * Sets the specification of endpoint ending at a flowlet.
   *
   * @param to endpoint specification.
   */
  public void setTo(FlowletStreamDefinitionImpl to) {
    this.to = to;
  }
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("from", from)
        .add("to", to)
        .toString();
  }
}
