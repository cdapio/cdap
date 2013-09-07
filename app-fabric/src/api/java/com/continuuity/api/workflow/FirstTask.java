/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.workflow;

/**
 *
 * @param <T>
 */
public interface FirstTask<T> {

  T startWith(Runnable task);
}
