package com.continuuity.observer;

/**
 *
 */
public enum StateChangeType {
  DEPLOYED(1),
  STARTING(2),
  STARTED(3),
  RUNNING(4),
  STOPPING(5),
  STOPPED(6),
  FAILED(7);

  int type;

  StateChangeType(int type) {
    this.type = type;
  }

  public int getType() {
    return this.type;
  }
}
