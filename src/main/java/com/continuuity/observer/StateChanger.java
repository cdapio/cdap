package com.continuuity.observer;

/**
 *
 */
public interface StateChanger {
  public void change(StateChangeData data) throws StateChangeException;
}
