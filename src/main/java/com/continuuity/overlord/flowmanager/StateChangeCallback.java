package com.continuuity.overlord.flowmanager;

/**
 *
 */
public interface StateChangeCallback<T> {
  public void process(T data);
}
