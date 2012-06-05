package com.continuuity.overlord.flowmanager;

import com.netflix.curator.framework.CuratorFramework;

import java.io.Closeable;

/**
 *
 */
public interface StateChangeListener extends Closeable {
  public void monitor(String path, StateChangeCallback callback) throws StateChangeListenerException;
}
