package com.continuuity.overlord.flowmanager;

/**
 *
 */
public interface StateChangeCallback {
  public void process(StateChangeData data);
}
