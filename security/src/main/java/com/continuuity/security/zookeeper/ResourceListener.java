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
   * Invoked on an update to an individual resource.
   * @param name the key for the resource being updated
   * @param instance the resource instance which was updated
   */
  void onResourceUpdate(String name, T instance);

  /**
   * Invoked when a resource is removed from the shared cache.
   * @param name the key for the resource that has been removed
   */
  void onResourceDelete(String name);

  /**
   * Invoked when an error occurs in one of the resource operations.
   * @param name the key for the resource on which the error occurred
   * @param throwable the exception that was thrown
   */
  void onError(String name, Throwable throwable);
}
