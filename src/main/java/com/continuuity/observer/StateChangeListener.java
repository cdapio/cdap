package com.continuuity.observer;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeListener<T> extends Closeable {
  public void listen(String path, StateChangeCallback<T> callback) throws StateChangeListenerException;
}
