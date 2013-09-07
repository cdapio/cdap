/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 * @param <T>
 */
public interface MoreTask<T> {

  MoreTask<T> then(Runnable task);

  T last(Runnable task);
}
