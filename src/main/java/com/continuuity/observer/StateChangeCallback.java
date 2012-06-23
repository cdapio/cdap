package com.continuuity.observer;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeCallback<T> extends Closeable {
  public void init(String uri) throws Exception;
  public void process(T data);
}
