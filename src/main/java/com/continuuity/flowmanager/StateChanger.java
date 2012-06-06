package com.continuuity.flowmanager;

/**
 *
 */
public interface StateChanger {
  public void change(StateChangeData data) throws StateChangeException;
}
