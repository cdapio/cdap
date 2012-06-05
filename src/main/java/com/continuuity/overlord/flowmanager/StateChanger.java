package com.continuuity.overlord.flowmanager;

/**
 *
 */
public interface StateChanger {
  public void change(StateChangeData data) throws StateChangeException;
}
