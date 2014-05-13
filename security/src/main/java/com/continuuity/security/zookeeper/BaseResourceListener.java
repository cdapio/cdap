package com.continuuity.security.zookeeper;

/**
 * Simple {@link ResourceListener} implementation with no-op implementations.  Implementers interested only
 * in handling specific events can subclass this class and override the handler methods that they care about.
 * @param <T> The resource type being managed by {@code SharedResourceCache}.
 */
public class BaseResourceListener<T> implements ResourceListener<T> {
  @Override
  public void onUpdate() {
    // no-op
  }

  @Override
  public void onResourceUpdate(String name, T instance) {
    // no-op
  }

  @Override
  public void onResourceDelete(String name) {
    // no-op
  }

  @Override
  public void onError(String name, Throwable throwable) {
    // no-op
  }
}
