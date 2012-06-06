package com.continuuity.observer;

/**
 *
 */
public class StateChangeException extends Exception {
  public StateChangeException(String reason) {
    super(reason);
  }
}
