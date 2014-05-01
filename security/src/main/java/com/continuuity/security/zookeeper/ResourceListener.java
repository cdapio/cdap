package com.continuuity.security.zookeeper;

/**
 * Allows a client to receive notifications when the resources managed by {@link SharedResourceCache}
 * are updated.
 * @param <T> The resource type being managed by {@code SharedResourceCache}.
 */
public interface ResourceListener<T> {
  /**
   * Invoked when the entire set of cached resources has changed.
   */
  void onUpdate();

  /**
   * Invoked on an update to an individual resource.  If the resource has been removed from the cache, then
   * the passed {@code instance} value will be {@code null}.
   * @param name the key for the resource being updated
   * @param instance the resource instance which was updated (null if the resource was removed)
   */
  void onResourceUpdate(String name, T instance);
}
