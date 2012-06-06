package com.continuuity.observer;

/**
 *
 */
public class StateChangeListenerException extends Exception {
  public StateChangeListenerException(String message) {
    super(message);
  }

  public StateChangeListenerException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public StateChangeListenerException(Throwable throwable) {
    super(throwable);
  }
}
