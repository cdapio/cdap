package com.continuuity.flowmanager;

/**
 *
 */
public enum StateChangeType {
  DEPLOYED_FLOW (1),
  STARTING_FLOW (2),
  RUNNING_FLOW (3),
  STOPPING_FLOW (4),
  HEARTBEAT_FLOW (5),
  STOPPED_FLOW (6);

  int type;
  StateChangeType(int type) {
    this.type = type;
  }

  public int getType() {
    return this.type;
  }
}
