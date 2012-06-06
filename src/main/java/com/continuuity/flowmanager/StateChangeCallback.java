package com.continuuity.flowmanager;

/**
 *
 */
public interface StateChangeCallback<T> {
  public void process(T data);
}
