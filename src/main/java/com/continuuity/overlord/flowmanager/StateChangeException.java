package com.continuuity.overlord.flowmanager;

/**
 *
 */
public class StateChangeException extends Exception {
  public StateChangeException(String reason) {
    super(reason);
  }
}
