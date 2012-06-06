package com.continuuity.observer;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeCallback<T> extends Closeable {
  public void process(T data);
}
