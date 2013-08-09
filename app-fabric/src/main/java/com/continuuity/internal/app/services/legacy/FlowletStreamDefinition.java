package com.continuuity.internal.app.services.legacy;

/**
 * Provides the definition for providing an endpoint of an edge connecting two flowlets,
 * or connecting a flowlet with a stream.
 *
 * If the flowlet name is set, then this connects to the stream of that flowlet with the given stream name
 * If the flowlet name is null, then this connects to named stream of the enclosing flow
 */
public interface FlowletStreamDefinition {
  /**
   * Returns the name of the flowlet, or null is the connection is to a flow stream
   * @return name of the flowlet.
   */
  public String getFlowlet();

  /**
   * Return the name of the stream.
   * @return the name of the stream.
   */
  public String getStream();

  /**
   * @return whether this connects to a stream of a flowlet
   */
  public boolean isFlowletStream();

  /**
   * @return whether this connects to a stream the enclosing flow
   */
  public boolean isFlowStream();
}
