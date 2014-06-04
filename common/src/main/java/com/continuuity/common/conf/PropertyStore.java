/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Cancellable;

import java.io.Closeable;

/**
 * Represents store for properties/configurations. It allows properties being store and shared.
 *
 * @param <T> Type of property
 */
public interface PropertyStore<T> extends Closeable {

  /**
   * Performs conditional update (compare and set) on a give property.
   *
   * @param name Name of the property
   * @param updater Updater to provides updated property value. The updater might get called multiple times.
   * @return A future that will be completed when the update is completed and will carry the property value being
   *         saved to property store.
   */
  ListenableFuture<T> update(String name, PropertyUpdater<T> updater);

  /**
   * Sets the property value unconditionally.
   *
   * @param name Name of the property
   * @param property Property value to set
   * @return A future that will be completed when the property is saved to property store. The future will carry
   *         the property value being saved when completed.
   */
  ListenableFuture<T> set(String name, T property);

  /**
   * Adds a listener for listening changes on a given property. Calls to listener is guaranteed to be serialized.
   *
   * @param name Name of the property
   * @param listener listener to receive changes in property
   * @return A {@link Cancellable} to cancel listening
   */
  Cancellable addChangeListener(String name, PropertyChangeListener<T> listener);
}
