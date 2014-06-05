/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An in memory implementation of {@link PropertyStore}.
 *
 * @param <T> Type of property
 */
public final class InMemoryPropertyStore<T> extends AbstractPropertyStore<T> {

  @Override
  protected boolean listenerAdded(String name) {
    return true;
  }

  @Override
  public synchronized ListenableFuture<T> update(String name, PropertyUpdater<T> updater) {
    try {
      T property = updater.apply(getCached(name)).get();
      updateAndNotify(name, property);
      return Futures.immediateFuture(property);
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  public synchronized ListenableFuture<T> set(String name, T property) {
    updateAndNotify(name, property);
    return Futures.immediateFuture(property);
  }
}
