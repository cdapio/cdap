package com.continuuity.observer;

/**
 *
 */
public enum StateChangeType {
  DEPLOYED (1),
  STARTING (2),
  RUNNING (3),
  STOPPING (4),
  HEARTBEAT (5),
  STOPPED (6);

  int type;
  StateChangeType(int type) {
    this.type = type;
  }

  public int getType() {
    return this.type;
  }
}
