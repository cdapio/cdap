package com.continuuity.test;

/**
 *
 */
public interface FlowManager {

  void setFlowletInstances(String flowletName, int instances);

  void stop();
}
