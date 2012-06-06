package com.continuuity.flowmanager.flowmanager;

/**
 *
 */
public interface StateChangeCallback<T> {
  public void process(T data);
}
