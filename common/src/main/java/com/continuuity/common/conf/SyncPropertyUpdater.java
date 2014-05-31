/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * A {@link com.continuuity.common.conf.PropertyUpdater} that computes property value synchronously.
 *
 * @param <T> Type of property value
 */
public abstract class SyncPropertyUpdater<T> implements PropertyUpdater<T> {

  @Override
  public final ListenableFuture<T> apply(@Nullable T property) throws Exception {
    return Futures.immediateFuture(compute(property));
  }

  protected abstract T compute(@Nullable T property);
}
