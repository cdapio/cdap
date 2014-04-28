package com.continuuity.security.zookeeper;

/**
 * Allows a client to receive notifications when the resources managed by {@link SharedResourceCache}
 * are updated.
 * @param <T> The resource type being managed by {@code SharedResourceCache}.
 */
public interface ResourceListener<T> {
  void onUpdate();
  void onResourceUpdate(T instance);
}
