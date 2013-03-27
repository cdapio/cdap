package com.continuuity.api.data.batch;

/**
 * Utility class to define object with {@code key} property.
 */
public interface WithKey<T> {
  /**
   * @return key
   */
  T getKey();
}
