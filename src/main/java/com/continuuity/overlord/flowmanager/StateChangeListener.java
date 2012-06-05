package com.continuuity.overlord.flowmanager;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeListener<T> extends Closeable {
  public void monitor(String path, StateChangeCallback<T> callback) throws StateChangeListenerException;
}
