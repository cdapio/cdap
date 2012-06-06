package com.continuuity.flowmanager.flowmanager;

/**
 *
 */
public interface StateChanger {
  public void change(StateChangeData data) throws StateChangeException;
}
