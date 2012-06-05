package com.continuuity.overlord.flowmanager;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeListener<T> extends Closeable {
  public void listen(String path, StateChangeCallback<T> callback) throws StateChangeListenerException;
}
