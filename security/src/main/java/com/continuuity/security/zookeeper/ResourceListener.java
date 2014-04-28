package com.continuuity.security.zookeeper;

/**
 *
 */
public interface ResourceListener<T> {
  void onUpdate();
  void onResourceUpdate(T instance);
}
