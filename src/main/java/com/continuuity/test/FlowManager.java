package com.continuuity.test;

/**
 * Instance for this class is for managing a running {@link com.continuuity.api.flow.Flow Flow}.
 */
public interface FlowManager {

  /**
   * Changes the number of flowlet instances.
   *
   * @param flowletName Name of the flowlet.
   * @param instances Number of instances to change to.
   */
  void setFlowletInstances(String flowletName, int instances);

  /**
   * Stops the running flow.
   */
  void stop();
}
