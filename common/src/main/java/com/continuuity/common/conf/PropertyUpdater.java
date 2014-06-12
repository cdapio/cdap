/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * An async function to update property in {@link PropertyStore}.
 *
 * @param <T> Type of property to update
 */
public interface PropertyUpdater<T> extends AsyncFunction<T, T> {

  /**
   * Computes the updated copy of property asynchronously.
   *
   * @param property The existing property value or {@code null} if no existing value.
   * @return A future that will be completed and carries the new property value when it become available.
   */
  @Override
  ListenableFuture<T> apply(@Nullable T property) throws Exception;
}
