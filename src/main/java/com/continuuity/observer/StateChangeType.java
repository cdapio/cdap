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
  FAILED(7),
  DELETED(8),
  RECONFIGURED(9);

  int type;

  StateChangeType(int type) {
    this.type = type;
  }

  public int getType() {
    return this.type;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static StateChangeType value(int value) {
    switch (value) {
      case 1:
        return DEPLOYED;
      case 2:
        return STARTING;
      case 3:
        return STARTED;
      case 4:
        return RUNNING;
      case 5:
        return STOPPING;
      case 6:
        return STOPPED;
      case 7:
        return FAILED;
      case 8:
        return DELETED;
      case 9:
        return RECONFIGURED;
      default:
        return null;
    }
  }

}
