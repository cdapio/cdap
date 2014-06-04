/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

/**
 * A listener for watching changes in {@link PropertyStore}.
 *
 * @param <T> Type of property
 */
public interface PropertyChangeListener<T> {

  /**
   * Invoked when the property changed.
   *
   * @param name Name of the property
   * @param property The updated property or {@link null} if the property is deleted.
   */
  void onChange(String name, T property);

  /**
   * Invoked when there is failure when listening for changes.
   *
   * @param failureCause The cause of the failure
   */
  void onError(String name, Throwable failureCause);
}
